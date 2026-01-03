"""Sourcify Sync - Download files from Sourcify export manifest using aria2c."""

import argparse
import sys
from pathlib import Path

from config import Config
from downloader import download_files
from logging_setup import get_logger, setup_logging, write_progress
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

    # Logging verbosity (mutually exclusive)
    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output (DEBUG level)",
    )
    verbosity_group.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Quiet mode (only warnings and errors)",
    )

    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Write logs to file (always DEBUG level)",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Setup logging first
    verbosity = 1 if args.verbose else (-1 if args.quiet else 0)
    setup_logging(verbosity=verbosity, log_file=args.log_file)
    logger = get_logger()

    logger.debug("Loading configuration...")
    config = Config.load(
        config_path=args.config,
        download_dir_override=args.download_dir,
        manifest_url_override=args.manifest_url,
        concurrency_override=args.concurrency,
        integrity_retry_count_override=args.integrity_retries,
        concurrent_validations_override=args.concurrent_validations,
    )

    logger.info("Manifest URL: %s", config.manifest_url)
    logger.info("Download directory: %s", config.download_dir)
    logger.info("Concurrent downloads: %s", config.concurrent_downloads)
    logger.info("Integrity check: %s", "enabled" if config.integrity_check else "disabled")
    logger.info("Integrity retries: %s", config.integrity_retry_count)
    logger.info("Concurrent validations: %s", config.concurrent_validations)
    if args.run_integrity:
        logger.info("Pre-download integrity check: enabled")

    logger.debug("Fetching manifest...")
    try:
        manifest = fetch_manifest(config.manifest_url)
    except Exception as e:
        logger.error("Error fetching manifest: %s", e)
        return 1

    file_paths = extract_file_paths(manifest)
    logger.info("Found %d files in manifest", len(file_paths))

    def on_verify_start(total: int) -> None:
        logger.debug("Verifying files...")

    def on_verify_progress(completed: int, total: int) -> None:
        percent = completed / total
        bar_width = 40
        filled = int(bar_width * percent)
        bar = "█" * filled + "░" * (bar_width - filled)
        write_progress(f"Verifying: [{bar}] {completed}/{total}")

    def on_verify_complete(to_download: int) -> None:
        print()  # Newline after progress bar
        logger.info("Found %d files to download", to_download)
        if to_download > 0:
            logger.info("Starting download...")

    def on_integrity_start(total: int) -> None:
        print()  # Newline before integrity check
        logger.debug("Verifying parquet file integrity...")

    def on_integrity_progress(completed: int, total: int) -> None:
        percent = completed / total
        bar_width = 40
        filled = int(bar_width * percent)
        bar = "█" * filled + "░" * (bar_width - filled)
        write_progress(f"Integrity: [{bar}] {completed}/{total}")

    def on_integrity_complete(failed: int) -> None:
        print()  # Newline after progress bar
        if failed > 0:
            logger.warning("Found %d corrupt files, re-downloading...", failed)
        else:
            logger.info("All files passed integrity check")

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

    logger.info("")
    logger.info("=" * 50)
    logger.info("Download Summary")
    logger.info("=" * 50)
    logger.info("Total files in manifest: %d", result.total_files)
    logger.info("Already complete: %d", result.skipped_files)
    logger.info("Downloaded/resumed: %d", result.to_download)

    if result.integrity_retries > 0:
        logger.info("Integrity retries: %d", result.integrity_retries)

    if result.integrity_failures > 0:
        logger.warning("Integrity failures: %d", result.integrity_failures)
        logger.warning("Some files failed integrity checks after max retries.")

    if result.aria2c_exit_code != 0:
        logger.warning("aria2c exit code: %d", result.aria2c_exit_code)
        logger.info("Note: Session saved. Run again to resume incomplete downloads.")
        return result.aria2c_exit_code

    if result.integrity_failures > 0:
        logger.warning("Sync completed with errors.")
        return 1

    logger.info("All files synced successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
