"""Manifest fetching and parsing for sourcify-sync."""

import httpx


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
            all_paths.extend(paths)

    return all_paths
