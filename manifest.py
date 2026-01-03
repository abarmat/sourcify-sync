"""Manifest fetching and parsing for sourcify-sync."""

import logging

import httpx

logger = logging.getLogger(__name__)


def validate_path(path: str) -> bool:
    """Validate that a path doesn't contain directory traversal sequences.

    Rejects paths with:
    - Parent directory references (..)
    - Absolute Unix paths (starting with /)
    - Absolute Windows paths (e.g., C:\\)
    """
    # Reject paths with parent directory references
    if ".." in path:
        return False
    # Reject absolute Unix paths
    if path.startswith("/"):
        return False
    # Reject absolute Windows paths (e.g., C:\, D:\)
    if len(path) > 1 and path[1] == ":":
        return False
    return True


def fetch_manifest(manifest_url: str) -> dict:
    """Fetch manifest JSON from the given URL."""
    response = httpx.get(manifest_url, timeout=30.0)
    response.raise_for_status()
    return response.json()


def extract_file_paths(manifest: dict) -> list[str]:
    """Extract all file paths from manifest.

    The manifest structure is:
    {
        "timestamp": ...,
        "dateStr": ...,
        "files": {
            "category1": ["path1", "path2", ...],
            "category2": ["path1", "path2", ...],
            ...
        }
    }

    Returns a flat list of all file paths.
    """
    files = manifest.get("files", {})
    all_paths = []

    for category, paths in files.items():
        if isinstance(paths, list):
            for path in paths:
                if validate_path(path):
                    all_paths.append(path)
                else:
                    logger.warning(f"Skipping invalid path: {path}")

    return all_paths
