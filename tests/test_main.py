"""Tests for main.py."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

from main import parse_args, main
from downloader import DownloadResult


class TestParseArgs:
    """Tests for parse_args()."""

    def test_default_values(self):
        """Uses default values when no args provided."""
        with patch.object(sys, "argv", ["main.py"]):
            args = parse_args()

        assert args.config is None
        assert args.download_dir is None
        assert args.manifest_url is None
        assert args.concurrency is None

    def test_all_args_provided(self):
        """Parses all provided arguments."""
        with patch.object(
            sys,
            "argv",
            [
                "main.py",
                "-c", "/path/to/config.toml",
                "-d", "/path/to/downloads",
                "-m", "https://example.com/manifest.json",
                "-j", "10",
            ],
        ):
            args = parse_args()

        assert args.config == Path("/path/to/config.toml")
        assert args.download_dir == "/path/to/downloads"
        assert args.manifest_url == "https://example.com/manifest.json"
        assert args.concurrency == 10

    def test_config_file_override(self):
        """Parses config file path."""
        with patch.object(sys, "argv", ["main.py", "--config", "custom.toml"]):
            args = parse_args()

        assert args.config == Path("custom.toml")


class TestMain:
    """Tests for main()."""

    def test_returns_zero_on_success(self, tmp_path):
        """Returns 0 exit code on successful execution."""
        with patch.object(sys, "argv", ["main.py"]), \
             patch("main.Config.load") as mock_config, \
             patch("main.fetch_manifest") as mock_fetch, \
             patch("main.extract_file_paths") as mock_extract, \
             patch("main.download_files") as mock_download:

            mock_config.return_value = MagicMock(
                manifest_url="https://example.com/manifest.json",
                download_dir=tmp_path / "downloads",
                concurrent_downloads=5,
            )
            mock_fetch.return_value = {"files": {}}
            mock_extract.return_value = []
            mock_download.return_value = DownloadResult(
                total_files=0,
                skipped_files=0,
                to_download=0,
                aria2c_exit_code=0,
            )

            exit_code = main()

        assert exit_code == 0

    def test_returns_one_on_manifest_fetch_error(self, tmp_path, capsys):
        """Returns 1 exit code when manifest fetch fails."""
        with patch.object(sys, "argv", ["main.py"]), \
             patch("main.Config.load") as mock_config, \
             patch("main.fetch_manifest") as mock_fetch:

            mock_config.return_value = MagicMock(
                manifest_url="https://example.com/manifest.json",
                download_dir=tmp_path / "downloads",
                concurrent_downloads=5,
            )
            mock_fetch.side_effect = Exception("Network error")

            exit_code = main()

        assert exit_code == 1
        captured = capsys.readouterr()
        assert "Error fetching manifest" in captured.err
