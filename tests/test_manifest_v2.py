"""Tests for manifest_v2.py - XML listing parser."""

from datetime import datetime, timezone

import pytest

from file_info import FileInfo
from manifest_v2 import parse_listing_xml, fetch_v2_listing


class TestParseListingXml:
    """Tests for parse_listing_xml()."""

    def test_parse_single_entry(self):
        """Parses single file entry from XML."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <Name>bucket</Name>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>v2/code/code_0_100000.parquet</Key>
                <LastModified>2024-01-15T10:30:00.000Z</LastModified>
                <ETag>"abc123def456"</ETag>
                <Size>1048576</Size>
            </Contents>
        </ListBucketResult>
        """
        files, next_marker, is_truncated = parse_listing_xml(xml)

        assert len(files) == 1
        assert files[0].key == "v2/code/code_0_100000.parquet"
        assert files[0].filename == "code_0_100000.parquet"
        assert files[0].size == 1048576
        assert files[0].etag == "abc123def456"  # Quotes stripped
        assert files[0].last_modified == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        assert is_truncated is False
        assert next_marker is None

    def test_parse_multiple_entries(self):
        """Parses multiple file entries."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>v2/code/code_0_100000.parquet</Key>
                <Size>1000</Size>
            </Contents>
            <Contents>
                <Key>v2/code/code_100001_200000.parquet</Key>
                <Size>2000</Size>
            </Contents>
            <Contents>
                <Key>v2/metadata/metadata_0_100000.parquet</Key>
                <Size>3000</Size>
            </Contents>
        </ListBucketResult>
        """
        files, _, _ = parse_listing_xml(xml)

        assert len(files) == 3
        assert files[0].filename == "code_0_100000.parquet"
        assert files[1].filename == "code_100001_200000.parquet"
        assert files[2].filename == "metadata_0_100000.parquet"

    def test_pagination_fields(self):
        """Extracts NextMarker and IsTruncated correctly."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>true</IsTruncated>
            <NextMarker>v2/code/code_200000_300000.parquet</NextMarker>
            <Contents>
                <Key>v2/code/code_0_100000.parquet</Key>
                <Size>1000</Size>
            </Contents>
        </ListBucketResult>
        """
        files, next_marker, is_truncated = parse_listing_xml(xml)

        assert is_truncated is True
        assert next_marker == "v2/code/code_200000_300000.parquet"
        assert len(files) == 1

    def test_filters_non_parquet_files(self):
        """Skips non-.parquet files in listing."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>v2/code/code_0_100000.parquet</Key>
                <Size>1000</Size>
            </Contents>
            <Contents>
                <Key>v2/readme.txt</Key>
                <Size>500</Size>
            </Contents>
            <Contents>
                <Key>v2/index.html</Key>
                <Size>200</Size>
            </Contents>
        </ListBucketResult>
        """
        files, _, _ = parse_listing_xml(xml)

        assert len(files) == 1
        assert files[0].filename == "code_0_100000.parquet"

    def test_etag_quotes_stripped(self):
        """ETag quotes are removed."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>v2/code/test.parquet</Key>
                <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
                <Size>0</Size>
            </Contents>
        </ListBucketResult>
        """
        files, _, _ = parse_listing_xml(xml)

        assert files[0].etag == "d41d8cd98f00b204e9800998ecf8427e"

    def test_handles_missing_optional_fields(self):
        """Handles entries with missing optional fields."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>v2/code/test.parquet</Key>
            </Contents>
        </ListBucketResult>
        """
        files, _, _ = parse_listing_xml(xml)

        assert len(files) == 1
        assert files[0].filename == "test.parquet"
        assert files[0].size is None
        assert files[0].etag is None
        assert files[0].last_modified is None


class TestFetchV2Listing:
    """Tests for fetch_v2_listing()."""

    def test_single_page(self, httpx_mock):
        """Fetches complete listing in one request."""
        xml_response = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>v2/code/code_0_100000.parquet</Key>
                <Size>1000</Size>
                <ETag>"abc123"</ETag>
            </Contents>
        </ListBucketResult>
        """
        httpx_mock.add_response(text=xml_response)

        files = fetch_v2_listing("https://example.com/", "v2/")

        assert len(files) == 1
        assert files[0].filename == "code_0_100000.parquet"
        assert files[0].etag == "abc123"

    def test_pagination(self, httpx_mock):
        """Handles multi-page listing correctly."""
        page1 = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>true</IsTruncated>
            <NextMarker>v2/code/code_100000_200000.parquet</NextMarker>
            <Contents>
                <Key>v2/code/code_0_100000.parquet</Key>
                <Size>1000</Size>
            </Contents>
        </ListBucketResult>
        """
        page2 = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>v2/code/code_100000_200000.parquet</Key>
                <Size>2000</Size>
            </Contents>
        </ListBucketResult>
        """
        httpx_mock.add_response(text=page1)
        httpx_mock.add_response(text=page2)

        files = fetch_v2_listing("https://example.com/", "v2/")

        assert len(files) == 2
        assert files[0].filename == "code_0_100000.parquet"
        assert files[1].filename == "code_100000_200000.parquet"

    def test_page_callback(self, httpx_mock):
        """Calls on_page_fetched callback correctly."""
        xml_response = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>v2/code/test.parquet</Key>
                <Size>1000</Size>
            </Contents>
        </ListBucketResult>
        """
        httpx_mock.add_response(text=xml_response)

        callback_args = []

        def callback(page, total):
            callback_args.append((page, total))

        fetch_v2_listing("https://example.com/", "v2/", on_page_fetched=callback)

        assert callback_args == [(1, 1)]


class TestFileInfo:
    """Tests for FileInfo dataclass."""

    def test_has_checksum_with_etag(self):
        """has_checksum returns True when ETag present."""
        info = FileInfo(
            key="v2/code/test.parquet",
            filename="test.parquet",
            etag="abc123",
        )
        assert info.has_checksum is True

    def test_has_checksum_without_etag(self):
        """has_checksum returns False when ETag is None."""
        info = FileInfo(
            key="code/test.parquet",
            filename="test.parquet",
            etag=None,
        )
        assert info.has_checksum is False
