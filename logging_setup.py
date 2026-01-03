"""Logging configuration for sourcify-sync."""

import logging
import sys
from pathlib import Path


def setup_logging(
    verbosity: int = 0,
    log_file: Path | None = None,
) -> logging.Logger:
    """Configure and return the logger for sourcify-sync.

    Args:
        verbosity: -1 for quiet (WARNING+), 0 for normal (INFO), 1 for verbose (DEBUG)
        log_file: Optional path to write logs to file

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("sourcify_sync")
    logger.setLevel(logging.DEBUG)

    # Clear any existing handlers
    logger.handlers.clear()

    # Console handler with level based on verbosity
    console_handler = logging.StreamHandler(sys.stdout)
    if verbosity < 0:
        console_handler.setLevel(logging.WARNING)
    elif verbosity > 0:
        console_handler.setLevel(logging.DEBUG)
    else:
        console_handler.setLevel(logging.INFO)

    # Simple format for console (no timestamp in normal mode)
    if verbosity > 0:
        console_fmt = logging.Formatter("%(levelname)s: %(message)s")
    else:
        console_fmt = logging.Formatter("%(message)s")
    console_handler.setFormatter(console_fmt)
    logger.addHandler(console_handler)

    # File handler (always DEBUG level, with timestamps)
    if log_file:
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setLevel(logging.DEBUG)
        file_fmt = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_fmt)
        logger.addHandler(file_handler)

    return logger


def get_logger() -> logging.Logger:
    """Get the sourcify_sync logger instance."""
    return logging.getLogger("sourcify_sync")


def write_progress(message: str) -> None:
    """Write a progress bar update directly to terminal.

    This bypasses logging entirely for progress updates that use
    carriage return for in-place updates.
    """
    sys.stdout.write(f"\r{message}")
    sys.stdout.flush()
