"""aria2c-based downloader for sourcify-sync with robust resume support."""

import os
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from collections.abc import Callable
from pathlib import Path

from config import Config
from logging_setup import get_logger


@dataclass
class DownloadResult:
    total_files: int
    skipped_files: int
    to_download: int
    aria2c_exit_code: int
    integrity_failures: int = 0
    integrity_retries: int = 0


def get_files_to_download(
    file_paths: list[str],
    base_url: str,
    download_dir: Path,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[list[tuple[str, str]], dict[str, int]]:
    """Determine which files need to be downloaded by checking local existence.

    Trust local files: if a file exists with size > 0, it's considered complete.
    No HEAD requests are made - this is reliable and fast.

    Returns:
        - list of (url, local_filename) tuples for files that need downloading
        - updated cache dict with local file sizes
    """
    to_download: list[tuple[str, str]] = []
    updated_cache: dict[str, int] = {}
    total = len(file_paths)

    for i, relative_path in enumerate(file_paths):
        filename = os.path.basename(relative_path)
        local_path = download_dir / filename
        url = f"{base_url}{relative_path}"

        if local_path.exists():
            local_size = local_path.stat().st_size
            if local_size > 0:
                # File exists and has content - trust it
                updated_cache[filename] = local_size
                if on_progress:
                    on_progress(i + 1, total)
                continue

        # File missing or empty - needs download
        to_download.append((url, filename))
        if on_progress:
            on_progress(i + 1, total)

    return to_download, updated_cache


def load_session_urls(session_file: Path) -> set[str]:
    """Load URLs from existing aria2c session file.

    Session file format has URLs on lines that don't start with whitespace.
    """
    urls = set()

    if not session_file.exists():
        return urls

    try:
        with open(session_file) as f:
            for line in f:
                line = line.strip()
                if line and line.startswith("http"):
                    urls.add(line)
    except OSError as e:
        logger = get_logger()
        logger.debug("Failed to read session file %s: %s", session_file, e)

    return urls


def create_aria2c_input_file(files_to_download: list[tuple[str, str]]) -> Path:
    """Create aria2c input file with URLs and output filenames.

    aria2c input format:
    URL
      out=filename
    URL
      out=filename
    ...
    """
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="aria2c_input_")

    with os.fdopen(fd, "w") as f:
        for url, filename in files_to_download:
            f.write(f"{url}\n")
            f.write(f"  out={filename}\n")

    return Path(path)


def verify_parquet_integrity(
    download_dir: Path,
    filenames: list[str],
    on_progress: Callable[[int, int], None] | None = None,
    max_workers: int = 4,
) -> list[str]:
    """Verify parquet files are valid by reading metadata and schema.

    Uses ThreadPoolExecutor for concurrent validation.
    Returns list of filenames that failed validation (corrupt files are deleted).
    """
    import pyarrow.parquet as pq

    failed: list[str] = []
    failed_lock = threading.Lock()
    progress_counter = 0
    progress_lock = threading.Lock()
    total = len(filenames)

    def validate_single_file(filename: str) -> tuple[str, bool]:
        """Validate a single parquet file. Returns (filename, is_valid)."""
        filepath = download_dir / filename
        if not filepath.exists() or filepath.suffix != ".parquet":
            return (filename, True)  # Skip non-parquet or missing files
        try:
            pq.read_metadata(filepath)  # Validate file structure/footer
            pq.read_schema(filepath)  # Validate column definitions
            return (filename, True)
        except pq.lib.ArrowInvalid as e:
            # Parquet file is corrupt - delete and retry
            logger = get_logger()
            logger.warning("Parquet file corrupt %s: %s", filename, e)
            try:
                filepath.unlink()
            except OSError as unlink_err:
                logger.debug("Failed to delete corrupt file %s: %s", filename, unlink_err)
            return (filename, False)
        except Exception as e:
            # System error (permissions, memory, etc.) - don't delete, just report
            logger = get_logger()
            logger.error("Failed to validate %s (not deleting): %s", filename, e)
            return (filename, False)

    def update_progress() -> None:
        """Thread-safe progress update."""
        nonlocal progress_counter
        with progress_lock:
            progress_counter += 1
            current = progress_counter
        if on_progress:
            on_progress(current, total)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(validate_single_file, filename): filename
            for filename in filenames
        }
        for future in as_completed(futures):
            filename, is_valid = future.result()
            if not is_valid:
                with failed_lock:
                    failed.append(filename)
            update_progress()

    return failed


def run_aria2c(
    config: Config,
    input_file: Path,
) -> int:
    """Run aria2c with the given input file.

    Returns aria2c exit code.
    """
    config.download_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        config.aria2c_path,
        "-c",  # Continue/resume partial downloads
        f"--save-session={config.session_file}",  # Save state on exit
        "--save-session-interval=10",  # Save every 10 seconds
        f"-j{config.concurrent_downloads}",  # Concurrent downloads
        f"-d{config.download_dir}",  # Download directory
        "--auto-file-renaming=false",  # Don't rename on conflict
        "--console-log-level=notice",  # Show progress
        "--summary-interval=5",  # Summary every 5 seconds
        #    "--file-allocation=falloc",  # It is recommended for newer file systems like ext4 (with extents enabled), btrfs, or xfs
        f"-i{input_file}",  # Input file
    ]

    result = subprocess.run(cmd)
    return result.returncode


def download_files_impl(
    config: Config,
    file_paths: list[str],
    on_verify_start: Callable[[int], None] | None = None,
    on_verify_progress: Callable[[int, int], None] | None = None,
    on_verify_complete: Callable[[int], None] | None = None,
    on_integrity_start: Callable[[int], None] | None = None,
    on_integrity_progress: Callable[[int, int], None] | None = None,
    on_integrity_complete: Callable[[int], None] | None = None,
    max_integrity_retries: int = 3,
    integrity_check: bool = True,
    run_integrity: bool = False,
    dry_run: bool = False,
) -> DownloadResult:
    """Download files using aria2c with robust resume support and integrity checking.

    Returns DownloadResult with statistics.
    """
    total_files = len(file_paths)

    # Run pre-download integrity check if requested (skip in dry-run mode)
    if run_integrity and not dry_run:
        config.download_dir.mkdir(parents=True, exist_ok=True)
        # Only validate files that are in the manifest (safe to delete and re-download)
        manifest_filenames = {os.path.basename(p) for p in file_paths}
        existing_files = [
            f.name for f in config.download_dir.glob("*.parquet")
            if f.is_file() and f.name in manifest_filenames
        ]
        if existing_files:
            if on_integrity_start:
                on_integrity_start(len(existing_files))

            failed = verify_parquet_integrity(
                config.download_dir,
                existing_files,
                on_progress=on_integrity_progress,
                max_workers=config.concurrent_validations,
            )

            if on_integrity_complete:
                on_integrity_complete(len(failed))

    if on_verify_start:
        on_verify_start(total_files)

    # Get files that need downloading (missing or empty locally)
    files_to_download, _ = get_files_to_download(
        file_paths,
        config.base_url,
        config.download_dir,
        on_progress=on_verify_progress,
    )

    # Load any incomplete downloads from previous session
    session_urls = load_session_urls(config.session_file)

    # Add session URLs that aren't already in our list
    existing_urls = {url for url, _ in files_to_download}
    for session_url in session_urls:
        if session_url not in existing_urls:
            # Extract filename from URL
            filename = os.path.basename(session_url)
            files_to_download.append((session_url, filename))

    if on_verify_complete:
        on_verify_complete(len(files_to_download))

    skipped_files = total_files - len(files_to_download)
    initial_to_download = len(files_to_download)

    if not files_to_download:
        # Clean up session file if everything is complete
        if config.session_file.exists():
            config.session_file.unlink()

        return DownloadResult(
            total_files=total_files,
            skipped_files=skipped_files,
            to_download=0,
            aria2c_exit_code=0,
        )

    # Dry run: return without downloading
    if dry_run:
        return DownloadResult(
            total_files=total_files,
            skipped_files=skipped_files,
            to_download=initial_to_download,
            aria2c_exit_code=0,
        )

    # Build URL lookup for re-downloads
    url_by_filename: dict[str, str] = {
        filename: url for url, filename in files_to_download
    }

    exit_code = 0
    integrity_retries = 0
    permanent_failures: list[str] = []

    while files_to_download:
        input_file = create_aria2c_input_file(files_to_download)

        try:
            exit_code = run_aria2c(config, input_file)
        finally:
            input_file.unlink(missing_ok=True)

        if exit_code != 0:
            # aria2c failed, don't run integrity check
            break

        # Skip integrity check if disabled
        if not integrity_check:
            break

        # Run integrity check on downloaded files
        downloaded_filenames = [filename for _, filename in files_to_download]

        if on_integrity_start:
            on_integrity_start(len(downloaded_filenames))

        failed_files = verify_parquet_integrity(
            config.download_dir,
            downloaded_filenames,
            on_progress=on_integrity_progress,
            max_workers=config.concurrent_validations,
        )

        if on_integrity_complete:
            on_integrity_complete(len(failed_files))

        if not failed_files:
            # All files passed integrity check
            break

        integrity_retries += 1

        if integrity_retries >= max_integrity_retries:
            # Max retries reached, record permanent failures
            permanent_failures.extend(failed_files)
            break

        # Rebuild download list for failed files
        files_to_download = [
            (url_by_filename[filename], filename)
            for filename in failed_files
            if filename in url_by_filename
        ]

    # Clean up session file if download completed successfully
    if exit_code == 0 and config.session_file.exists():
        config.session_file.unlink()

    return DownloadResult(
        total_files=total_files,
        skipped_files=skipped_files,
        to_download=initial_to_download,
        aria2c_exit_code=exit_code,
        integrity_failures=len(permanent_failures),
        integrity_retries=integrity_retries,
    )


def download_files(
    config: Config,
    file_paths: list[str],
    on_verify_start: Callable[[int], None] | None = None,
    on_verify_progress: Callable[[int, int], None] | None = None,
    on_verify_complete: Callable[[int], None] | None = None,
    on_integrity_start: Callable[[int], None] | None = None,
    on_integrity_progress: Callable[[int, int], None] | None = None,
    on_integrity_complete: Callable[[int], None] | None = None,
    integrity_check: bool = True,
    run_integrity: bool = False,
    max_integrity_retries: int = 3,
    dry_run: bool = False,
) -> DownloadResult:
    """Download files, checking local existence to determine what needs downloading."""
    return download_files_impl(
        config,
        file_paths,
        on_verify_start,
        on_verify_progress,
        on_verify_complete,
        on_integrity_start,
        on_integrity_progress,
        on_integrity_complete,
        max_integrity_retries=max_integrity_retries,
        integrity_check=integrity_check,
        run_integrity=run_integrity,
        dry_run=dry_run,
    )
