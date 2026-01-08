"""v2 format: GCP XML listing parser with pagination support."""

from collections.abc import Callable
from datetime import datetime
from xml.etree import ElementTree as ET

import httpx

from file_info import FileInfo
from logging_setup import get_logger

logger = get_logger()


def parse_listing_xml(xml_content: str) -> tuple[list[FileInfo], str | None, bool]:
    """Parse GCP XML listing response.

    Args:
        xml_content: Raw XML response from GCP listing endpoint.

    Returns:
        Tuple of:
        - List of FileInfo objects for parquet files
        - NextMarker for pagination (None if complete)
        - IsTruncated flag
    """
    root = ET.fromstring(xml_content)

    # GCP uses the S3 ListBucket response format with namespace
    # Try with namespace first, fall back to no namespace
    ns = {"s3": "http://doc.s3.amazonaws.com/2006-03-01"}
    files = []

    # Try namespaced first, then non-namespaced
    contents = root.findall(".//s3:Contents", ns)
    if not contents:
        contents = root.findall(".//Contents")

    for content in contents:
        # Handle both namespaced and non-namespaced elements
        key = content.findtext("s3:Key", namespaces=ns) or content.findtext("Key")
        if not key or not key.endswith(".parquet"):
            continue

        # Extract filename from key (e.g., "v2/code/code_0_100000.parquet" -> "code_0_100000.parquet")
        filename = key.rsplit("/", 1)[-1]

        # Parse optional fields (try namespaced first)
        size_text = content.findtext("s3:Size", namespaces=ns) or content.findtext(
            "Size"
        )
        etag = content.findtext("s3:ETag", namespaces=ns) or content.findtext("ETag")
        last_mod = content.findtext(
            "s3:LastModified", namespaces=ns
        ) or content.findtext("LastModified")

        files.append(
            FileInfo(
                key=key,
                filename=filename,
                size=int(size_text) if size_text else None,
                etag=etag.strip('"') if etag else None,  # Remove surrounding quotes
                last_modified=(
                    datetime.fromisoformat(last_mod.replace("Z", "+00:00"))
                    if last_mod
                    else None
                ),
            )
        )

    # Pagination (try namespaced first)
    is_truncated_text = (
        root.findtext("s3:IsTruncated", namespaces=ns)
        or root.findtext("IsTruncated")
        or ""
    )
    is_truncated = is_truncated_text.lower() == "true"
    next_marker = root.findtext("s3:NextMarker", namespaces=ns) or root.findtext(
        "NextMarker"
    )
    return files, next_marker, is_truncated


def fetch_v2_listing(
    listing_url: str,
    prefix: str,
    on_page_fetched: Callable[[int, int], None] | None = None,
) -> list[FileInfo]:
    """Fetch all files from v2 XML listing with pagination.

    Args:
        listing_url: Base URL for listing (e.g., "https://export.sourcify.dev/")
        prefix: Prefix filter (e.g., "v2/")
        on_page_fetched: Optional callback(page_num, files_so_far)

    Returns:
        List of all FileInfo objects for parquet files.
    """
    all_files: list[FileInfo] = []
    marker: str | None = None
    page = 0

    while True:
        # Build URL with query parameters
        params: dict[str, str] = {"prefix": prefix}
        if marker:
            params["marker"] = marker

        logger.debug("Fetching v2 listing page %d (marker=%s)", page + 1, marker)
        response = httpx.get(listing_url, params=params, timeout=60.0)
        response.raise_for_status()

        files, next_marker, is_truncated = parse_listing_xml(response.text)
        all_files.extend(files)
        page += 1

        if on_page_fetched:
            on_page_fetched(page, len(all_files))

        logger.debug(
            "Page %d: %d files (total: %d, truncated: %s)",
            page,
            len(files),
            len(all_files),
            is_truncated,
        )

        if not is_truncated:
            break

        # For pagination, use the last key if NextMarker not provided
        if next_marker:
            marker = next_marker
        elif files:
            marker = files[-1].key
        else:
            break

    return all_files
