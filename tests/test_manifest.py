"""Tests for manifest.py."""

import httpx
import pytest

from manifest import fetch_manifest, extract_file_paths, validate_path


class TestFetchManifest:
    """Tests for fetch_manifest()."""

    def test_fetch_manifest_success(self, httpx_mock):
        """Successfully fetches and parses manifest JSON."""
        expected = {"files": {"code": ["file1.parquet"]}}
        httpx_mock.add_response(
            url="https://example.com/manifest.json",
            json=expected,
        )

        result = fetch_manifest("https://example.com/manifest.json")

        assert result == expected

    def test_fetch_manifest_http_error_404(self, httpx_mock):
        """Raises exception on 404 error."""
        httpx_mock.add_response(
            url="https://example.com/manifest.json",
            status_code=404,
        )

        with pytest.raises(httpx.HTTPStatusError):
            fetch_manifest("https://example.com/manifest.json")

    def test_fetch_manifest_http_error_500(self, httpx_mock):
        """Raises exception on 500 error."""
        httpx_mock.add_response(
            url="https://example.com/manifest.json",
            status_code=500,
        )

        with pytest.raises(httpx.HTTPStatusError):
            fetch_manifest("https://example.com/manifest.json")

    def test_fetch_manifest_timeout(self, httpx_mock):
        """Raises exception on timeout."""
        httpx_mock.add_exception(
            httpx.TimeoutException("Connection timeout"),
            url="https://example.com/manifest.json",
        )

        with pytest.raises(httpx.TimeoutException):
            fetch_manifest("https://example.com/manifest.json")


class TestExtractFilePaths:
    """Tests for extract_file_paths()."""

    def test_extract_file_paths_multiple_categories(self, sample_manifest):
        """Extracts paths from multiple categories."""
        result = extract_file_paths(sample_manifest)

        assert len(result) == 3
        assert "code/code_0_100000.parquet" in result
        assert "code/code_100001_200000.parquet" in result
        assert "metadata/metadata_0_100000.parquet" in result

    def test_extract_file_paths_empty_manifest(self):
        """Returns empty list for empty manifest."""
        result = extract_file_paths({})

        assert result == []

    def test_extract_file_paths_missing_files_key(self):
        """Returns empty list when 'files' key is missing."""
        manifest = {"timestamp": 123, "dateStr": "2024-01-01"}

        result = extract_file_paths(manifest)

        assert result == []

    def test_extract_file_paths_skips_non_list_values(self):
        """Skips category values that are not lists."""
        manifest = {
            "files": {
                "valid": ["file1.parquet"],
                "invalid": "not-a-list",
            }
        }

        result = extract_file_paths(manifest)

        assert result == ["file1.parquet"]

    def test_extract_file_paths_filters_directory_traversal(self):
        """Filters out paths with directory traversal attempts."""
        manifest = {
            "files": {
                "code": [
                    "code/file1.parquet",
                    "../../../etc/passwd",
                    "code/../secret.parquet",
                ]
            }
        }

        result = extract_file_paths(manifest)

        assert result == ["code/file1.parquet"]

    def test_extract_file_paths_filters_absolute_paths(self):
        """Filters out absolute paths."""
        manifest = {
            "files": {
                "code": [
                    "code/file1.parquet",
                    "/etc/passwd",
                    "/absolute/path.parquet",
                ]
            }
        }

        result = extract_file_paths(manifest)

        assert result == ["code/file1.parquet"]


class TestValidatePath:
    """Tests for validate_path()."""

    def test_valid_simple_path(self):
        """Accepts simple relative paths."""
        assert validate_path("file.parquet") is True

    def test_valid_nested_path(self):
        """Accepts nested relative paths."""
        assert validate_path("code/file.parquet") is True
        assert validate_path("a/b/c/file.parquet") is True

    def test_rejects_parent_directory_traversal(self):
        """Rejects paths with .. directory traversal."""
        assert validate_path("../file.parquet") is False
        assert validate_path("code/../file.parquet") is False
        assert validate_path("../../../etc/passwd") is False

    def test_rejects_absolute_unix_path(self):
        """Rejects absolute Unix paths."""
        assert validate_path("/etc/passwd") is False
        assert validate_path("/absolute/path.parquet") is False

    def test_rejects_absolute_windows_path(self):
        """Rejects absolute Windows paths."""
        assert validate_path("C:\\file.parquet") is False
        assert validate_path("D:\\path\\file.parquet") is False
