# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import concurrent.futures
import dataclasses
import functools
import json
import random
import sys
import time
import os
import boto3
import boto3.session
from botocore.exceptions import ClientError
from typing import cast, Any, Callable, Dict, List, Optional, TypeVar

from deadline.job_attachments.download import download_files_from_manifests
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests import BaseAssetManifest
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
)
from deadline_worker_agent.sessions.attachment_models import WorkerManifestProperties
from deadline_worker_agent.aws.deadline import (
    record_attachment_download_fail_telemetry_event,
    record_attachment_download_latencies_telemetry_event,
    record_attachment_download_telemetry_event,
    record_success_fail_telemetry_event,
)

_queue_id = os.environ.get("DEADLINE_QUEUE_ID", "queue-unknown")  # Just for telemetry

# During a SYNC_INPUT_JOB_ATTACHMENTS session action, the transfer rate is periodically reported through
# a callback function. If a transfer rate lower than LOW_TRANSFER_RATE_THRESHOLD is observed in a series
# for LOW_TRANSFER_COUNT_THRESHOLD times, it is considered concerning or potentially stalled, and the
# session action is canceled.
LOW_TRANSFER_RATE_THRESHOLD = 10 * 10**3  # 10 KB/s÷
LOW_TRANSFER_COUNT_THRESHOLD = (
    60  # Each progress report takes 1 sec at the longest, so 60 reports amount to 1 min in total.
)


F = TypeVar("F", bound=Callable[..., Any])


def failure_telemetry(function: F) -> F:
    """Decorator to record failure telemetry on a function"""

    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return function(*args, **kwargs)
        except Exception as e:
            record_attachment_download_fail_telemetry_event(
                queue_id=_queue_id,
                failure_reason=f"{function.__name__}: {type(e).__name__}",
            )
            raise e

    return cast(F, wrapper)


def _seconds_to_minutes_str(seconds: int) -> str:
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    if minutes > 0 and remaining_seconds > 0:
        return f"{minutes} minute{'s' if minutes != 1 else ''} {remaining_seconds} second{'s' if remaining_seconds != 1 else ''}"
    elif minutes > 0:
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif remaining_seconds == 0:
        return "0 seconds"
    else:
        return f"{remaining_seconds} second{'s' if remaining_seconds != 1 else ''}"


@dataclasses.dataclass
class AttachmentDownloadLatencies:
    """Stores metrics for function latencies in this script"""

    load_worker_manifest_properties: int = 0
    build_merged_manifests_by_root: int = 0
    ensure_cache_populated: int = 0
    perform_download: int = 0
    total: int = 0


# Concurrency for the cross-region "does this object exist? if not, copy" pass.
# S3 cross-region CopyObject is IO-bound and can sustain many parallel requests.
CACHE_SYNC_MAX_WORKERS = 32


@failure_telemetry
def load_worker_manifest_properties(worker_properties_file: str) -> List[WorkerManifestProperties]:
    """
    Load and parse worker manifest properties from a JSON file.

    Args:
        worker_properties_file: Path to the worker properties JSON file

    Returns:
        List of WorkerManifestProperties objects

    Raises:
        FileNotFoundError: If the worker properties file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
        ValueError: If the JSON structure is invalid
    """
    with open(worker_properties_file, "r") as f:
        worker_manifest_properties_data = json.load(f)

    if isinstance(worker_manifest_properties_data, dict):
        raise ValueError("Expected a list but got a dictionary in worker properties file")

    return [WorkerManifestProperties.from_dict(item) for item in worker_manifest_properties_data]


@failure_telemetry
def build_merged_manifests_by_root(
    worker_manifest_properties: List[WorkerManifestProperties],
) -> Dict[str, BaseAssetManifest]:
    """
    Build a dictionary mapping local root paths to decoded asset manifests.

    Args:
        worker_manifest_properties: List of WorkerManifestProperties objects

    Returns:
        Dictionary mapping local_root_path to BaseAssetManifest objects

    Raises:
        FileNotFoundError: If a manifest file doesn't exist
        ValueError: If manifest decoding fails
    """
    manifests_by_root = {}

    for worker_prop in worker_manifest_properties:
        if worker_prop.local_input_manifest_path:
            with open(worker_prop.local_input_manifest_path, "r") as manifest_file:
                manifests_by_root[worker_prop.local_root_path] = decode_manifest(
                    manifest_file.read()
                )
        else:
            print(f"Root {worker_prop.root_path} contains no input manifest to sync.")

    return manifests_by_root


def _collect_cas_keys(
    cas_prefix: str,
    manifests_by_root: Dict[str, BaseAssetManifest],
) -> List[str]:
    """Return the deduplicated set of CAS S3 keys referenced by ``manifests_by_root``."""
    keys: set = set()
    for manifest in manifests_by_root.values():
        hash_alg = manifest.hashAlg.value  # type: ignore[attr-defined]
        for file in manifest.paths:
            keys.add(f"{cas_prefix}/{file.hash}.{hash_alg}")
    return list(keys)


def _copy_if_missing(
    s3_client: Any,
    cache_bucket: str,
    home_bucket: str,
    key: str,
) -> str:
    """Head the object in ``cache_bucket``; on 404 copy from ``home_bucket``.

    Returns one of ``"present"``, ``"copied"``, ``"already-copied"``. ``"already-copied"``
    is returned when the copy target was created by another process between the
    head and the copy attempt (raced with a peer).
    """
    try:
        s3_client.head_object(Bucket=cache_bucket, Key=key)
        return "present"
    except ClientError as e:
        status = int(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        code = e.response.get("Error", {}).get("Code", "")
        if status != 404 and code not in ("404", "NoSuchKey", "NotFound"):
            raise

    s3_client.copy_object(
        Bucket=cache_bucket,
        Key=key,
        CopySource={"Bucket": home_bucket, "Key": key},
    )
    return "copied"


@failure_telemetry
def ensure_cache_populated(
    s3_client: Any,
    cache_settings: JobAttachmentS3Settings,
    home_settings: JobAttachmentS3Settings,
    manifests_by_root: Dict[str, BaseAssetManifest],
) -> Dict[str, int]:
    """For each CAS object referenced by ``manifests_by_root``, ensure it exists in
    ``cache_settings``' bucket, copying from ``home_settings`` on miss.

    The key ordering is randomized so that two satellite workers syncing the same
    manifest are less likely to attempt to copy the same object at the same time
    (see the MRF cache-sync design doc).
    """
    cache_bucket = cache_settings.s3BucketName
    home_bucket = home_settings.s3BucketName
    cache_cas_prefix = cache_settings.full_cas_prefix()
    home_cas_prefix = home_settings.full_cas_prefix()

    if cache_cas_prefix != home_cas_prefix:
        # We currently assume both buckets use the same rootPrefix so a CAS
        # key can be reused verbatim. The service enforces this today; if it
        # ever diverges we would need to translate keys per bucket here.
        raise ValueError(
            f"Home and cache job attachment rootPrefixes differ ({home_cas_prefix!r} vs "
            f"{cache_cas_prefix!r}); cross-region cache sync requires them to match."
        )

    keys = _collect_cas_keys(cache_cas_prefix, manifests_by_root)
    random.shuffle(keys)

    counts = {"present": 0, "copied": 0}
    if not keys:
        return counts

    print(
        f"Multi-region cache: ensuring {len(keys)} object(s) are present in "
        f"s3://{cache_bucket}/{cache_cas_prefix}/ (copying missing objects from "
        f"s3://{home_bucket}/{home_cas_prefix}/)"
    )

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=CACHE_SYNC_MAX_WORKERS) as executor:
        futures = [
            executor.submit(_copy_if_missing, s3_client, cache_bucket, home_bucket, key)
            for key in keys
        ]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            counts[result] = counts.get(result, 0) + 1

    elapsed = time.perf_counter() - start
    print(
        f"Multi-region cache: {counts.get('copied', 0)} copied, "
        f"{counts.get('present', 0)} already present ({elapsed:.1f}s)"
    )
    return counts


@failure_telemetry
def perform_download(
    s3_settings: JobAttachmentS3Settings,
    manifests_by_root: Dict[str, BaseAssetManifest],
) -> DownloadSummaryStatistics:
    """
    Perform the actual download of files from S3 using the provided manifests.

    Args:
        s3_settings: S3 settings for the download
        manifests_by_root: Dictionary mapping root paths to manifests
        queue_id: Queue ID for telemetry

    Returns:
        DownloadSummaryStatistics object containing download results
    """
    low_transfer_count = 0
    last_processed_files = 0

    def progress_handler(job_attachments_download_status: ProgressReportMetadata) -> bool:
        """
        Callback for Job Attachments' download_files_from_manifests() to track the download progress.
        Returns True if the operation should continue as normal or False to cancel.
        """
        # Check the transfer rate from the progress report. It monitors for a series of
        # alarmingly low transfer rates, and if the count exceeds the specified threshold,
        # cancels the download and fails the current (SYNC_INPUT_JOB_ATTACHMENTS) action.
        nonlocal low_transfer_count, last_processed_files
        transfer_rate = job_attachments_download_status.transferRate

        # File completion acts as heartbeat to prevent false positive of low transfer rate
        # due to files with small sizes < ~50bytes
        if job_attachments_download_status.processedFiles > last_processed_files:
            low_transfer_count = 0
        last_processed_files = job_attachments_download_status.processedFiles

        if transfer_rate < LOW_TRANSFER_RATE_THRESHOLD:
            low_transfer_count += 1
        else:
            low_transfer_count = 0
        if low_transfer_count >= LOW_TRANSFER_COUNT_THRESHOLD:
            fail_message = (
                f"Input syncing failed due to successive low transfer rates (< {LOW_TRANSFER_RATE_THRESHOLD / 1000} KB/s). "
                f"The transfer rate was below the threshold for the last {_seconds_to_minutes_str(LOW_TRANSFER_COUNT_THRESHOLD)}."
            )
            print(f"openjd_fail: {fail_message}", flush=True)
            record_attachment_download_fail_telemetry_event(
                queue_id=_queue_id,
                failure_reason=f"Insufficient download speed: {fail_message}",
            )
            return False

        print(f"openjd_progress: {job_attachments_download_status.progress}")
        print(f"openjd_status: {job_attachments_download_status.progressMessage}")
        sys.stdout.flush()
        return True

    download_summary_statistics = download_files_from_manifests(
        s3_bucket=s3_settings.s3BucketName,
        manifests_by_root=manifests_by_root,
        cas_prefix=s3_settings.full_cas_prefix(),
        session=boto3.session.Session(),
        on_downloading_files=progress_handler,
    )
    record_attachment_download_telemetry_event(
        queue_id=_queue_id,
        summary=download_summary_statistics.convert_to_summary_statistics(),
    )
    print(f"Summary Statistics for file downloads:\n{download_summary_statistics}")
    return download_summary_statistics


@record_success_fail_telemetry_event(metric_name="attachment_download")
def main() -> None:
    """Main function to handle command line execution."""
    total_start_time = time.perf_counter_ns()

    parser = argparse.ArgumentParser()
    parser.add_argument("-s3", "--s3-uri", type=str, help="S3 root URI", required=True)
    parser.add_argument(
        "-wp",
        "--worker-properties",
        type=str,
        help="Worker manifest properties file",
        required=False,
    )
    parser.add_argument(
        "-hs3",
        "--home-s3-uri",
        type=str,
        help=(
            "Home-region S3 root URI. When provided, the worker is running in a "
            "satellite region: missing CAS objects are copied from this bucket to "
            "the regional cache bucket (--s3-uri) before downloading."
        ),
        required=False,
        default=None,
    )

    args = parser.parse_args()

    latencies = AttachmentDownloadLatencies()

    start_t = time.perf_counter_ns()
    worker_manifest_properties = load_worker_manifest_properties(args.worker_properties)
    latencies.load_worker_manifest_properties = time.perf_counter_ns() - start_t

    start_t = time.perf_counter_ns()
    manifests_by_root = build_merged_manifests_by_root(worker_manifest_properties)
    latencies.build_merged_manifests_by_root = time.perf_counter_ns() - start_t

    s3_settings = JobAttachmentS3Settings.from_s3_root_uri(args.s3_uri)
    home_s3_settings: Optional[JobAttachmentS3Settings] = (
        JobAttachmentS3Settings.from_s3_root_uri(args.home_s3_uri) if args.home_s3_uri else None
    )

    boto_session = boto3.session.Session()

    if home_s3_settings is not None:
        start_t = time.perf_counter_ns()
        ensure_cache_populated(
            s3_client=boto_session.client("s3"),
            cache_settings=s3_settings,
            home_settings=home_s3_settings,
            manifests_by_root=manifests_by_root,
        )
        latencies.ensure_cache_populated = time.perf_counter_ns() - start_t

    print("\nStarting download...")

    start_t = time.perf_counter_ns()
    perform_download(s3_settings, manifests_by_root)
    latencies.perform_download = time.perf_counter_ns() - start_t

    total = time.perf_counter_ns() - total_start_time
    latencies.total = total
    print(f"Finished downloading after {round(total * 10**-9, 2)} seconds")

    record_attachment_download_latencies_telemetry_event(
        queue_id=_queue_id,
        latencies=dataclasses.asdict(latencies),
    )


if __name__ == "__main__":
    main()
