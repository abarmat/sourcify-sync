"""Sourcify Sync - Download files from Sourcify export manifest using aria2c."""

import argparse
import sys
from pathlib import Path

from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from rich.table import Table

from config import Config
from downloader import download_files
from logging_setup import get_console, get_logger, setup_logging
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
    )
    parser.add_argument(
        "--concurrent-validations",
        type=int,
        default=None,
        help="Number of concurrent parquet validations (default: CPU count)",
    )
    parser.add_argument(
        "-n", "--dry-run",
        action="store_true",
        help="Preview what would be downloaded without downloading",
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

    console = get_console()

    logger.debug("Loading configuration...")
    config = Config.load(
        config_path=args.config,
        download_dir_override=args.download_dir,
        manifest_url_override=args.manifest_url,
        concurrency_override=args.concurrency,
        integrity_retry_count_override=args.integrity_retries,
        concurrent_validations_override=args.concurrent_validations,
    )

    # Display configuration in a styled panel
    config_table = Table(show_header=False, box=None, padding=(0, 1))
    config_table.add_column("Setting", style="dim")
    config_table.add_column("Value")
    config_table.add_row("Manifest URL", config.manifest_url)
    config_table.add_row("Download directory", str(config.download_dir))
    config_table.add_row("Concurrent downloads", str(config.concurrent_downloads))
    config_table.add_row("Integrity check", "[green]enabled[/]" if config.integrity_check else "[dim]disabled[/]")
    config_table.add_row("Integrity retries", str(config.integrity_retry_count))
    config_table.add_row("Concurrent validations", str(config.concurrent_validations))
    if args.run_integrity:
        config_table.add_row("Pre-download integrity", "[green]enabled[/]")

    console.print(Panel(config_table, title="Configuration", border_style="blue"))

    if args.dry_run:
        console.print()
        console.print("[yellow]DRY RUN MODE[/] - no files will be downloaded")
        console.print()

    logger.debug("Fetching manifest...")
    try:
        manifest = fetch_manifest(config.manifest_url)
    except Exception as e:
        console.print(f"[red]Error fetching manifest:[/] {e}")
        return 1

    file_paths = extract_file_paths(manifest)
    console.print(f"Found [cyan]{len(file_paths)}[/] files in manifest")

    # Progress bar state for callbacks
    progress_state: dict = {"progress": None, "task": None}

    def make_progress() -> Progress:
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=console,
        )

    def on_verify_start(total: int) -> None:
        logger.debug("Verifying files...")
        progress_state["progress"] = make_progress()
        progress_state["progress"].start()
        progress_state["task"] = progress_state["progress"].add_task("Verifying", total=total)

    def on_verify_progress(completed: int, total: int) -> None:
        progress_state["progress"].update(progress_state["task"], completed=completed)

    def on_verify_complete(to_download: int) -> None:
        progress_state["progress"].stop()
        console.print(f"Found [cyan]{to_download}[/] files to download")
        if to_download > 0 and not args.dry_run:
            console.print("[dim]Starting download...[/]")

    def on_integrity_start(total: int) -> None:
        console.print()
        logger.debug("Verifying parquet file integrity...")
        progress_state["progress"] = make_progress()
        progress_state["progress"].start()
        progress_state["task"] = progress_state["progress"].add_task("Integrity check", total=total)

    def on_integrity_progress(completed: int, total: int) -> None:
        progress_state["progress"].update(progress_state["task"], completed=completed)

    def on_integrity_complete(failed: int) -> None:
        progress_state["progress"].stop()
        if failed > 0:
            console.print(f"[yellow]Found {failed} corrupt files, re-downloading...[/]")
        else:
            console.print("[green]All files passed integrity check[/]")

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
        dry_run=args.dry_run,
    )

    # Build summary table
    console.print()
    summary_title = "Dry Run Summary" if args.dry_run else "Download Summary"
    summary_table = Table(title=summary_title, show_header=False)
    summary_table.add_column("Metric", style="dim")
    summary_table.add_column("Value", justify="right")

    summary_table.add_row("Total files in manifest", str(result.total_files))
    summary_table.add_row("Already complete", str(result.skipped_files))
    if args.dry_run:
        summary_table.add_row("Would download", str(result.to_download))
    else:
        summary_table.add_row("Downloaded/resumed", str(result.to_download))

    if not args.dry_run:
        if result.integrity_retries > 0:
            summary_table.add_row("Integrity retries", str(result.integrity_retries))

        if result.integrity_failures > 0:
            summary_table.add_row("Integrity failures", f"[red]{result.integrity_failures}[/]")

    console.print(summary_table)

    if not args.dry_run:
        if result.integrity_failures > 0:
            console.print("[yellow]Some files failed integrity checks after max retries.[/]")

        if result.aria2c_exit_code != 0:
            console.print(f"[yellow]aria2c exit code: {result.aria2c_exit_code}[/]")
            console.print("[dim]Session saved. Run again to resume incomplete downloads.[/]")
            return result.aria2c_exit_code

        if result.integrity_failures > 0:
            console.print("[yellow]Sync completed with errors.[/]")
            return 1

        console.print("[green]All files synced successfully![/]")

    return 0


if __name__ == "__main__":
    sys.exit(main())
