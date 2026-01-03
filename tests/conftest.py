"""Shared fixtures for sourcify-sync tests."""

import sys
from pathlib import Path

import pytest

# Add project root to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config


@pytest.fixture
def sample_manifest():
    """Sample manifest data for testing."""
    return {
        "timestamp": 1234567890,
        "dateStr": "2024-01-01",
        "files": {
            "code": [
                "code/code_0_100000.parquet",
                "code/code_100001_200000.parquet",
            ],
            "metadata": [
                "metadata/metadata_0_100000.parquet",
            ],
        },
    }


@pytest.fixture
def sample_config(tmp_path):
    """Pre-configured Config instance for testing."""
    return Config(
        manifest_url="https://example.com/manifest.json",
        download_dir=tmp_path / "downloads",
        aria2c_path="aria2c",
        concurrent_downloads=5,
        base_url="https://example.com/",
        integrity_check=True,
        integrity_retry_count=3,
        concurrent_validations=4,
    )


@pytest.fixture
def config_toml_content():
    """Sample config.toml content."""
    return """
manifest_url = "https://custom.example.com/manifest.json"
download_dir = "/tmp/custom-downloads"
aria2c_path = "/usr/bin/aria2c"
concurrent_downloads = 10
"""
