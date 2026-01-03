"""Tests for logging_setup module."""

import logging
from io import StringIO

import pytest
from logging_setup import setup_logging, get_logger, write_progress


class TestSetupLogging:
    """Tests for setup_logging()."""

    def test_default_level_is_info(self):
        """Default verbosity sets INFO level on console."""
        logger = setup_logging(verbosity=0)
        console_handler = logger.handlers[0]
        assert console_handler.level == logging.INFO

    def test_verbose_enables_debug(self):
        """Verbose mode sets DEBUG level."""
        logger = setup_logging(verbosity=1)
        console_handler = logger.handlers[0]
        assert console_handler.level == logging.DEBUG

    def test_quiet_sets_warning(self):
        """Quiet mode sets WARNING level."""
        logger = setup_logging(verbosity=-1)
        console_handler = logger.handlers[0]
        assert console_handler.level == logging.WARNING

    def test_file_handler_added(self, tmp_path):
        """Log file option adds file handler."""
        log_file = tmp_path / "test.log"
        logger = setup_logging(log_file=log_file)

        assert len(logger.handlers) == 2
        assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)

    def test_file_handler_is_debug_level(self, tmp_path):
        """File handler always captures DEBUG."""
        log_file = tmp_path / "test.log"
        logger = setup_logging(verbosity=-1, log_file=log_file)

        file_handler = [
            h for h in logger.handlers if isinstance(h, logging.FileHandler)
        ][0]
        assert file_handler.level == logging.DEBUG


class TestGetLogger:
    """Tests for get_logger()."""

    def test_returns_sourcify_sync_logger(self):
        """Returns the sourcify_sync logger."""
        logger = get_logger()
        assert logger.name == "sourcify_sync"


class TestWriteProgress:
    """Tests for write_progress()."""

    def test_writes_with_carriage_return(self, monkeypatch):
        """write_progress outputs with carriage return prefix."""
        output = StringIO()
        monkeypatch.setattr("sys.stdout", output)
        write_progress("Test progress")
        assert output.getvalue() == "\rTest progress"
