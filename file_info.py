"""File metadata dataclass for unified v1/v2 file information."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class FileInfo:
    """Metadata for a file to download.

    Attributes:
        key: Full path from listing (e.g., "v2/code/code_0_100000.parquet")
        filename: Just the filename (e.g., "code_0_100000.parquet")
        size: File size in bytes (v2 only, None for v1)
        etag: MD5 checksum without quotes (v2 only, None for v1)
        last_modified: Last modified timestamp (v2 only, None for v1)
    """

    key: str
    filename: str
    size: int | None = None
    etag: str | None = None
    last_modified: datetime | None = None

    @property
    def has_checksum(self) -> bool:
        """Check if this file has a checksum available."""
        return self.etag is not None
