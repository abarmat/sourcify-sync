"""Tests for manifest.py."""

import httpx
import pytest

from manifest import fetch_manifest, extract_file_paths


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
