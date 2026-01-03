"""Sourcify Sync - Download files from Sourcify export manifest using aria2c."""

import argparse
import sys
from pathlib import Path

from config import Config
from downloader import download_files
from manifest import extract_file_paths, fetch_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download files from Sourcify export manifest using aria2c",
    )
    parser.add_argument(
        "-c", "--config",
        type=Path,
        default=None,
        help="Path to config file (default: config.toml)",
    )
    parser.add_argument(
        "-d", "--download-dir",
        type=str,
        default=None,
        help="Override download directory from config",
    )
    parser.add_argument(
        "-m", "--manifest-url",
        type=str,
        default=None,
        help="Override manifest URL from config",
    )
    parser.add_argument(
        "-j", "--concurrency",
        type=int,
        default=None,
        help="Number of concurrent downloads",
    )
    parser.add_argument(
        "-r", "--run-integrity",
        action="store_true",
        default=False,
        help="Run integrity check on existing files before downloading",
    )
    parser.add_argument(
        "-i", "--integrity-retries",
        type=int,
        default=None,
        help="Number of times to retry downloading files that fail integrity checks",
        "-v", "--concurrent-validations",
        type=int,
        default=None,
        help="Number of concurrent parquet validations (default: CPU count)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print("Loading configuration...")
    config = Config.load(
        config_path=args.config,
        download_dir_override=args.download_dir,
        manifest_url_override=args.manifest_url,
        concurrency_override=args.concurrency,
        integrity_retry_count_override=args.integrity_retries,
        concurrent_validations_override=args.concurrent_validations,
    )

    print(f"Manifest URL: {config.manifest_url}")
    print(f"Download directory: {config.download_dir}")
    print(f"Concurrent downloads: {config.concurrent_downloads}")
    print(f"Concurrent validations: {config.concurrent_validations}")
    print(f"Integrity check: {'enabled' if config.integrity_check else 'disabled'}")
    print(f"Integrity retries: {config.integrity_retry_count}")
    if args.run_integrity:
        print("Pre-download integrity check: enabled")
    print()

    print("Fetching manifest...")
    try:
        manifest = fetch_manifest(config.manifest_url)
    except Exception as e:
        print(f"Error fetching manifest: {e}", file=sys.stderr)
        return 1

    file_paths = extract_file_paths(manifest)
    print(f"Found {len(file_paths)} files in manifest")
    print()

    def on_verify_start(total: int) -> None:
        print("Verifying files...")

    def on_verify_progress(completed: int, total: int) -> None:
        percent = completed / total
        bar_width = 40
        filled = int(bar_width * percent)
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"\rVerifying: [{bar}] {completed}/{total}", end="", flush=True)

    def on_verify_complete(to_download: int) -> None:
        print()  # Newline after progress bar
        print(f"Found {to_download} files to download")
        print()
        if to_download > 0:
            print("Starting download...")

    def on_integrity_start(total: int) -> None:
        print()
        print("Verifying parquet file integrity...")

    def on_integrity_progress(completed: int, total: int) -> None:
        percent = completed / total
        bar_width = 40
        filled = int(bar_width * percent)
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"\rIntegrity: [{bar}] {completed}/{total}", end="", flush=True)

    def on_integrity_complete(failed: int) -> None:
        print()  # Newline after progress bar
        if failed > 0:
            print(f"Found {failed} corrupt files, re-downloading...")
        else:
            print("All files passed integrity check")

    result = download_files(
        config,
        file_paths,
        on_verify_start=on_verify_start,
        on_verify_progress=on_verify_progress,
        on_verify_complete=on_verify_complete,
        on_integrity_start=on_integrity_start,
        on_integrity_progress=on_integrity_progress,
        on_integrity_complete=on_integrity_complete,
        integrity_check=config.integrity_check,
        run_integrity=args.run_integrity,
        max_integrity_retries=config.integrity_retry_count,
    )

    print()
    print("=" * 50)
    print("Download Summary")
    print("=" * 50)
    print(f"Total files in manifest: {result.total_files}")
    print(f"Already complete: {result.skipped_files}")
    print(f"Downloaded/resumed: {result.to_download}")

    if result.integrity_retries > 0:
        print(f"Integrity retries: {result.integrity_retries}")

    if result.integrity_failures > 0:
        print(f"Integrity failures: {result.integrity_failures}")
        print("Warning: Some files failed integrity checks after max retries.")

    if result.aria2c_exit_code != 0:
        print(f"aria2c exit code: {result.aria2c_exit_code}")
        print("Note: Session saved. Run again to resume incomplete downloads.")
        return result.aria2c_exit_code

    if result.integrity_failures > 0:
        print("Sync completed with errors.")
        return 1

    print("All files synced successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
