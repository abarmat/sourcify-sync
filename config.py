"""Configuration loading and validation for sourcify-sync."""

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
}


@dataclass
class Config:
    manifest_url: str
    download_dir: Path
    aria2c_path: str
    concurrent_downloads: int
    base_url: str

    @property
    def session_file(self) -> Path:
        """Path to aria2c session file for resume support."""
        return self.download_dir / ".aria2c-session"

    @classmethod
    def load(
        cls,
        config_path: Path | None = None,
        download_dir_override: str | None = None,
        manifest_url_override: str | None = None,
        concurrency_override: int | None = None,
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

        manifest_url = config_data["manifest_url"]
        parsed = urlparse(manifest_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{'/'.join(parsed.path.rsplit('/', 1)[:-1])}/"

        return cls(
            manifest_url=manifest_url,
            download_dir=Path(config_data["download_dir"]).expanduser().resolve(),
            aria2c_path=config_data["aria2c_path"],
            concurrent_downloads=int(config_data["concurrent_downloads"]),
            base_url=base_url,
        )
