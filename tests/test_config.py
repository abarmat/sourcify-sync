"""Tests for config.py."""

from pathlib import Path

from config import Config, DEFAULTS


class TestConfigLoad:
    """Tests for Config.load()."""

    def test_load_with_defaults_when_no_config_file(self, tmp_path):
        """Config uses defaults when config file doesn't exist."""
        config = Config.load(config_path=tmp_path / "nonexistent.toml")

        assert config.manifest_url == DEFAULTS["manifest_url"]
        assert config.aria2c_path == DEFAULTS["aria2c_path"]
        assert config.concurrent_downloads == DEFAULTS["concurrent_downloads"]
        assert config.integrity_retry_count == DEFAULTS["integrity_retry_count"]

    def test_load_from_toml_file(self, tmp_path, config_toml_content):
        """Config loads values from TOML file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_toml_content)

        config = Config.load(config_path=config_file)

        assert config.manifest_url == "https://custom.example.com/manifest.json"
        assert config.aria2c_path == "/usr/bin/aria2c"
        assert config.concurrent_downloads == 10

    def test_cli_overrides_take_precedence(self, tmp_path, config_toml_content):
        """CLI overrides take precedence over config file values."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_toml_content)

        config = Config.load(
            config_path=config_file,
            download_dir_override="/cli/override",
            manifest_url_override="https://cli.example.com/manifest.json",
            concurrency_override=20,
        )

        assert config.manifest_url == "https://cli.example.com/manifest.json"
        assert str(config.download_dir) == "/cli/override"
        assert config.concurrent_downloads == 20

    def test_integrity_retry_count_from_toml(self, tmp_path):
        """Config loads integrity_retry_count from TOML file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('integrity_retry_count = 5\n')

        config = Config.load(config_path=config_file)

        assert config.integrity_retry_count == 5

    def test_integrity_retry_count_cli_override(self, tmp_path):
        """CLI override for integrity_retry_count takes precedence."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('integrity_retry_count = 5\n')

        config = Config.load(
            config_path=config_file,
            integrity_retry_count_override=10,
        )

        assert config.integrity_retry_count == 10

    def test_base_url_derived_from_manifest_url(self, tmp_path):
        """Base URL is correctly derived from manifest URL."""
        config = Config.load(
            config_path=tmp_path / "nonexistent.toml",
            manifest_url_override="https://export.sourcify.dev/path/to/manifest.json",
        )

        assert config.base_url == "https://export.sourcify.dev/path/to/"

    def test_download_dir_path_expansion(self, tmp_path):
        """Download directory with ~ is expanded."""
        config = Config.load(
            config_path=tmp_path / "nonexistent.toml",
            download_dir_override="~/downloads",
        )

        assert "~" not in str(config.download_dir)
        assert config.download_dir.is_absolute()


class TestConfigProperties:
    """Tests for Config properties."""

    def test_session_file_property(self, sample_config):
        """session_file returns correct path."""
        expected = sample_config.download_dir / ".aria2c-session"
        assert sample_config.session_file == expected
