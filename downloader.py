"""aria2c-based downloader for sourcify-sync with robust resume support."""

import asyncio
import os
import subprocess
import tempfile
from dataclasses import dataclass
from collections.abc import Callable
from pathlib import Path

import httpx

from config import Config


@dataclass
class DownloadResult:
    total_files: int
    skipped_files: int
    to_download: int
    aria2c_exit_code: int


async def check_file_needs_download(
    client: httpx.AsyncClient,
    url: str,
    local_path: Path,
    semaphore: asyncio.Semaphore,
) -> bool:
    """Check if a file needs to be downloaded.

    Returns True if file is missing or size doesn't match remote.
    """
    if not local_path.exists():
        return True

    local_size = local_path.stat().st_size

    async with semaphore:
        try:
            response = await client.head(url, follow_redirects=True)
            response.raise_for_status()

            content_length = response.headers.get("content-length")
            if content_length is None:
                # Can't verify size, assume needs download if we got here
                return True

            remote_size = int(content_length)
            return local_size != remote_size

        except httpx.HTTPError:
            # On error, assume file needs download
            return True


async def get_files_to_download(
    file_paths: list[str],
    base_url: str,
    download_dir: Path,
    max_concurrent_checks: int = 50,
    on_progress: Callable[[int, int], None] | None = None,
) -> list[tuple[str, str]]:
    """Determine which files need to be downloaded using parallel HEAD requests.

    Returns list of (url, local_filename) tuples for files that need downloading.
    """
    semaphore = asyncio.Semaphore(max_concurrent_checks)
    to_download: list[tuple[str, str]] = []
    total = len(file_paths)
    completed = [0]  # Mutable container for counter

    async def check_with_progress(
        client: httpx.AsyncClient,
        url: str,
        local_path: Path,
    ) -> bool:
        """Wrapper that updates progress after each check."""
        result = await check_file_needs_download(client, url, local_path, semaphore)
        completed[0] += 1
        if on_progress:
            on_progress(completed[0], total)
        return result

    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = []

        for relative_path in file_paths:
            filename = os.path.basename(relative_path)
            local_path = download_dir / filename
            url = f"{base_url}{relative_path}"

            task = check_with_progress(client, url, local_path)
            tasks.append((url, filename, task))

        # Gather all results
        results = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)

        for (url, filename, _), needs_download in zip(tasks, results):
            if isinstance(needs_download, Exception):
                # On exception, include file for download
                to_download.append((url, filename))
            elif needs_download:
                to_download.append((url, filename))

    return to_download


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
    except OSError:
        pass

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
        "--file-allocation=falloc",  # It is recommended for newer file systems like ext4 (with extents enabled), btrfs, or xfs
        f"-i{input_file}",  # Input file
    ]

    result = subprocess.run(cmd)
    return result.returncode


async def download_files_async(
    config: Config,
    file_paths: list[str],
    on_verify_start: Callable[[int], None] | None = None,
    on_verify_progress: Callable[[int, int], None] | None = None,
    on_verify_complete: Callable[[int], None] | None = None,
) -> DownloadResult:
    """Download files using aria2c with robust resume support.

    Returns DownloadResult with statistics.
    """
    total_files = len(file_paths)

    if on_verify_start:
        on_verify_start(total_files)

    # Get files that need downloading (missing or incomplete)
    files_to_download = await get_files_to_download(
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

    input_file = create_aria2c_input_file(files_to_download)

    try:
        exit_code = run_aria2c(config, input_file)
    finally:
        input_file.unlink(missing_ok=True)

    # Clean up session file if download completed successfully
    if exit_code == 0 and config.session_file.exists():
        config.session_file.unlink()

    return DownloadResult(
        total_files=total_files,
        skipped_files=skipped_files,
        to_download=len(files_to_download),
        aria2c_exit_code=exit_code,
    )


def download_files(
    config: Config,
    file_paths: list[str],
    on_verify_start: Callable[[int], None] | None = None,
    on_verify_progress: Callable[[int, int], None] | None = None,
    on_verify_complete: Callable[[int], None] | None = None,
) -> DownloadResult:
    """Synchronous wrapper for download_files_async."""
    return asyncio.run(
        download_files_async(
            config,
            file_paths,
            on_verify_start,
            on_verify_progress,
            on_verify_complete,
        )
    )
