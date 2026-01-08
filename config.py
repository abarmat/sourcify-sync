"""Configuration loading and validation for sourcify-sync."""

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin, urlparse


DEFAULT_CONFIG_PATH = Path("config.toml")

DEFAULTS = {
    "manifest_url": "https://export.sourcify.dev/manifest.json",
    "download_dir": "./downloads",
    "aria2c_path": "aria2c",
    "concurrent_downloads": 5,
    "integrity_check": True,
    "integrity_retry_count": 3,
    "concurrent_validations": None,  # None = os.cpu_count() or 4
    "format_version": "v2",  # "v1" (JSON manifest) or "v2" (XML listing)
    "v2_listing_url": "https://export.sourcify.dev/",
    "v2_prefix": "v2/",
}


@dataclass
class Config:
    manifest_url: str
    download_dir: Path
    aria2c_path: str
    concurrent_downloads: int
    base_url: str
    integrity_check: bool
    integrity_retry_count: int
    concurrent_validations: int
    format_version: str
    v2_listing_url: str
    v2_prefix: str

    @property
    def download_subdir(self) -> Path:
        """Subdirectory for this format version (v1/ or v2/)."""
        return self.download_dir / self.format_version

    @property
    def session_file(self) -> Path:
        """Path to aria2c session file for resume support."""
        return self.download_subdir / ".aria2c-session"

    @classmethod
    def load(
        cls,
        config_path: Path | None = None,
        download_dir_override: str | None = None,
        manifest_url_override: str | None = None,
        concurrency_override: int | None = None,
        integrity_retry_count_override: int | None = None,
        concurrent_validations_override: int | None = None,
        format_version_override: str | None = None,
    ) -> "Config":
        """Load configuration from TOML file with defaults."""
        config_data = dict(DEFAULTS)

        path = config_path or DEFAULT_CONFIG_PATH
        if path.exists():
            with open(path, "rb") as f:
                file_config = tomllib.load(f)
                config_data.update(file_config)

        if download_dir_override:
            config_data["download_dir"] = download_dir_override
        if manifest_url_override:
            config_data["manifest_url"] = manifest_url_override
        if concurrency_override is not None:
            config_data["concurrent_downloads"] = concurrency_override
        if integrity_retry_count_override is not None:
            config_data["integrity_retry_count"] = integrity_retry_count_override
        if concurrent_validations_override is not None:
            config_data["concurrent_validations"] = concurrent_validations_override
        if format_version_override is not None:
            config_data["format_version"] = format_version_override

        # Resolve concurrent_validations default
        concurrent_validations = config_data.get("concurrent_validations")
        if concurrent_validations is None:
            concurrent_validations = os.cpu_count() or 4

        manifest_url = config_data["manifest_url"]
        parsed = urlparse(manifest_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{'/'.join(parsed.path.rsplit('/', 1)[:-1])}/"

        format_version = config_data["format_version"]
        if format_version not in ("v1", "v2"):
            raise ValueError(f"Invalid format_version: {format_version}. Must be 'v1' or 'v2'.")

        return cls(
            manifest_url=manifest_url,
            download_dir=Path(config_data["download_dir"]).expanduser().resolve(),
            aria2c_path=config_data["aria2c_path"],
            concurrent_downloads=int(config_data["concurrent_downloads"]),
            base_url=base_url,
            integrity_check=bool(config_data.get("integrity_check", True)),
            integrity_retry_count=int(config_data.get("integrity_retry_count", 3)),
            concurrent_validations=int(concurrent_validations),
            format_version=format_version,
            v2_listing_url=config_data["v2_listing_url"],
            v2_prefix=config_data["v2_prefix"],
        )
