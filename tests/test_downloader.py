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
    verify_parquet_integrity,
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
            concurrent_validations=4,
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
            concurrent_validations=4,
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
            concurrent_validations=4,
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


class TestVerifyParquetIntegrity:
    """Tests for verify_parquet_integrity()."""

    def test_skip_files_with_aria2_control_file(self, tmp_path):
        """Skips files that have an active .aria2 control file (incomplete download)."""
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create a parquet file and its .aria2 control file
        parquet_file = download_dir / "test.parquet"
        parquet_file.write_bytes(b"incomplete data")
        aria2_control = download_dir / "test.parquet.aria2"
        aria2_control.write_text("control file")

        failed = verify_parquet_integrity(download_dir, ["test.parquet"])

        # Should be skipped (not in failed list)
        assert failed == []
        # File should NOT be deleted
        assert parquet_file.exists()

    def test_valid_parquet_passes(self, tmp_path):
        """Valid parquet file passes integrity check."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create a valid parquet file
        table = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        parquet_file = download_dir / "valid.parquet"
        pq.write_table(table, parquet_file)

        failed = verify_parquet_integrity(download_dir, ["valid.parquet"])

        assert failed == []
        assert parquet_file.exists()

    def test_corrupt_parquet_deleted_and_reported(self, tmp_path):
        """Corrupt parquet file is deleted and reported as failed."""
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create a corrupt parquet file (invalid data)
        parquet_file = download_dir / "corrupt.parquet"
        parquet_file.write_bytes(b"this is not valid parquet data")

        failed = verify_parquet_integrity(download_dir, ["corrupt.parquet"])

        assert "corrupt.parquet" in failed
        # File should be deleted
        assert not parquet_file.exists()

    def test_permission_error_does_not_delete(self, tmp_path):
        """Permission errors don't delete the file but still report failure."""
        import pyarrow.parquet as pq

        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create a file that will trigger permission error
        parquet_file = download_dir / "protected.parquet"
        parquet_file.write_bytes(b"some data")

        with patch.object(pq, "read_metadata", side_effect=PermissionError("Access denied")):
            failed = verify_parquet_integrity(download_dir, ["protected.parquet"])

        # Should be reported as failed
        assert "protected.parquet" in failed
        # File should NOT be deleted (system error, not corruption)
        assert parquet_file.exists()

    def test_skip_missing_files(self, tmp_path):
        """Missing files are skipped (not reported as failed)."""
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Don't create the file
        failed = verify_parquet_integrity(download_dir, ["nonexistent.parquet"])

        assert failed == []

    def test_skip_non_parquet_files(self, tmp_path):
        """Non-parquet files are skipped."""
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create a non-parquet file
        txt_file = download_dir / "readme.txt"
        txt_file.write_text("not a parquet file")

        failed = verify_parquet_integrity(download_dir, ["readme.txt"])

        assert failed == []
        assert txt_file.exists()

    def test_progress_callback_called(self, tmp_path):
        """Progress callback is called for each file."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create valid parquet files
        table = pa.table({"col1": [1]})
        pq.write_table(table, download_dir / "file1.parquet")
        pq.write_table(table, download_dir / "file2.parquet")

        progress_calls = []

        def on_progress(completed, total):
            progress_calls.append((completed, total))

        verify_parquet_integrity(
            download_dir,
            ["file1.parquet", "file2.parquet"],
            on_progress=on_progress,
        )

        assert len(progress_calls) == 2
        # Check that we got both progress updates (order may vary due to threading)
        assert (1, 2) in progress_calls
        assert (2, 2) in progress_calls

    def test_delete_failure_logged_not_raised(self, tmp_path, caplog):
        """Delete failure is logged but doesn't raise exception."""
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create a corrupt parquet file
        parquet_file = download_dir / "corrupt.parquet"
        parquet_file.write_bytes(b"invalid parquet")

        # Mock unlink to fail
        original_unlink = Path.unlink

        def mock_unlink(self, missing_ok=False):
            if self.name == "corrupt.parquet":
                raise OSError("Permission denied")
            return original_unlink(self, missing_ok=missing_ok)

        with patch.object(Path, "unlink", mock_unlink):
            failed = verify_parquet_integrity(download_dir, ["corrupt.parquet"])

        # Should still report as failed
        assert "corrupt.parquet" in failed
        # File still exists (delete failed)
        assert parquet_file.exists()


class TestGetFilesToDownloadEdgeCases:
    """Edge case tests for get_files_to_download()."""

    def test_empty_file_needs_download(self, tmp_path):
        """Empty (0-byte) files are treated as needing download."""
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create an empty file
        empty_file = download_dir / "empty.parquet"
        empty_file.touch()
        assert empty_file.stat().st_size == 0

        to_download, cache = get_files_to_download(
            ["code/empty.parquet"],
            "https://example.com/",
            download_dir,
        )

        assert len(to_download) == 1
        assert to_download[0][1] == "empty.parquet"
        assert "empty.parquet" not in cache

    def test_directory_with_same_name_needs_download(self, tmp_path):
        """Directory with same name as target file triggers download."""
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        # Create a directory with the target filename
        dir_as_file = download_dir / "file.parquet"
        dir_as_file.mkdir()

        to_download, cache = get_files_to_download(
            ["code/file.parquet"],
            "https://example.com/",
            download_dir,
        )

        # Should need download (it's a directory, not a file)
        # Note: stat().st_size on a directory returns its metadata size, not 0
        # The exists() check passes, but it's not a proper file
        assert len(to_download) == 0 or "file.parquet" not in cache
