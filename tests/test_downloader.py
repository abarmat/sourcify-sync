"""Tests for downloader.py."""

from pathlib import Path
from unittest.mock import patch, MagicMock

from config import Config
from downloader import (
    get_files_to_download,
    load_session_urls,
    create_aria2c_input_file,
    run_aria2c,
    download_files,
    DownloadResult,
)


class TestGetFilesToDownload:
    """Tests for get_files_to_download()."""

    def test_all_files_need_download_empty_dir(self, tmp_path):
        """All files need download when directory is empty."""
        file_paths = ["code/file1.parquet", "code/file2.parquet"]
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        to_download, cache = get_files_to_download(
            file_paths, "https://example.com/", download_dir
        )

        assert len(to_download) == 2
        assert ("https://example.com/code/file1.parquet", "file1.parquet") in to_download
        assert ("https://example.com/code/file2.parquet", "file2.parquet") in to_download
        assert cache == {}

    def test_skip_existing_files(self, tmp_path):
        """Skips files that already exist with content."""
        file_paths = ["code/file1.parquet", "code/file2.parquet"]
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create one existing file with content
        (download_dir / "file1.parquet").write_text("content")

        to_download, cache = get_files_to_download(
            file_paths, "https://example.com/", download_dir
        )

        assert len(to_download) == 1
        assert to_download[0][1] == "file2.parquet"
        assert "file1.parquet" in cache

    def test_progress_callback_called(self, tmp_path):
        """Progress callback is called for each file."""
        file_paths = ["code/file1.parquet", "code/file2.parquet"]
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        progress_calls = []

        def on_progress(completed, total):
            progress_calls.append((completed, total))

        get_files_to_download(
            file_paths, "https://example.com/", download_dir, on_progress=on_progress
        )

        assert len(progress_calls) == 2
        assert progress_calls[0] == (1, 2)
        assert progress_calls[1] == (2, 2)


class TestLoadSessionUrls:
    """Tests for load_session_urls()."""

    def test_parse_existing_session_file(self, tmp_path):
        """Parses URLs from existing session file."""
        session_file = tmp_path / ".aria2c-session"
        session_file.write_text(
            "https://example.com/file1.parquet\n"
            "  out=file1.parquet\n"
            "https://example.com/file2.parquet\n"
            "  out=file2.parquet\n"
        )

        urls = load_session_urls(session_file)

        assert len(urls) == 2
        assert "https://example.com/file1.parquet" in urls
        assert "https://example.com/file2.parquet" in urls

    def test_handle_missing_session_file(self, tmp_path):
        """Returns empty set when session file doesn't exist."""
        session_file = tmp_path / "nonexistent-session"

        urls = load_session_urls(session_file)

        assert urls == set()


class TestCreateAria2cInputFile:
    """Tests for create_aria2c_input_file()."""

    def test_creates_correct_format(self):
        """Creates file with correct aria2c input format."""
        files = [
            ("https://example.com/file1.parquet", "file1.parquet"),
            ("https://example.com/file2.parquet", "file2.parquet"),
        ]

        input_file = create_aria2c_input_file(files)

        try:
            content = input_file.read_text()
            assert "https://example.com/file1.parquet\n" in content
            assert "  out=file1.parquet\n" in content
            assert "https://example.com/file2.parquet\n" in content
            assert "  out=file2.parquet\n" in content
        finally:
            input_file.unlink(missing_ok=True)

    def test_empty_list_creates_empty_file(self):
        """Creates empty file for empty list."""
        input_file = create_aria2c_input_file([])

        try:
            content = input_file.read_text()
            assert content == ""
        finally:
            input_file.unlink(missing_ok=True)


class TestRunAria2c:
    """Tests for run_aria2c()."""

    def test_builds_correct_command(self, tmp_path):
        """Builds correct aria2c command."""
        config = Config(
            manifest_url="https://example.com/manifest.json",
            download_dir=tmp_path / "downloads",
            aria2c_path="/usr/bin/aria2c",
            concurrent_downloads=10,
            base_url="https://example.com/",
            integrity_check=True,
            integrity_retry_count=3,
        )
        input_file = tmp_path / "input.txt"
        input_file.write_text("")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            exit_code = run_aria2c(config, input_file)

            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[0] == "/usr/bin/aria2c"
            assert "-c" in cmd
            assert "-j10" in cmd
            assert f"-d{config.download_dir}" in cmd
            assert f"-i{input_file}" in cmd
            assert exit_code == 0

    def test_creates_download_directory(self, tmp_path):
        """Creates download directory if it doesn't exist."""
        config = Config(
            manifest_url="https://example.com/manifest.json",
            download_dir=tmp_path / "new_downloads",
            aria2c_path="aria2c",
            concurrent_downloads=5,
            base_url="https://example.com/",
            integrity_check=True,
            integrity_retry_count=3,
        )
        input_file = tmp_path / "input.txt"
        input_file.write_text("")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            run_aria2c(config, input_file)

            assert config.download_dir.exists()


class TestDownloadFiles:
    """Tests for download_files()."""

    def test_returns_download_result_no_files_needed(self, tmp_path):
        """Returns DownloadResult when no files need downloading."""
        config = Config(
            manifest_url="https://example.com/manifest.json",
            download_dir=tmp_path / "downloads",
            aria2c_path="aria2c",
            concurrent_downloads=5,
            base_url="https://example.com/",
            integrity_check=True,
            integrity_retry_count=3,
        )
        config.download_dir.mkdir()

        # Create existing files
        (config.download_dir / "file1.parquet").write_text("content")

        result = download_files(config, ["code/file1.parquet"])

        assert isinstance(result, DownloadResult)
        assert result.total_files == 1
        assert result.skipped_files == 1
        assert result.to_download == 0
        assert result.aria2c_exit_code == 0
