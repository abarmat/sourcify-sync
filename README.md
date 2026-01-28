# Sourcify Sync

Download files from the Sourcify export manifest using aria2c.

## Requirements

- Python 3.13+
- [aria2c](https://aria2.github.io/) - high-performance download utility
- [just](https://github.com/casey/just) - command runner (optional, for development)

### Installing aria2c

**macOS:**
```bash
brew install aria2
```

**Ubuntu/Debian:**
```bash
apt install aria2
```

**Arch Linux:**
```bash
pacman -S aria2
```

## Installation

```bash
git clone <repository-url>
cd sourcify-sync
uv sync
```

## Usage

```bash
# Run with default configuration
uv run python main.py

# Override download directory
uv run python main.py -d /path/to/downloads

# Use a custom config file
uv run python main.py -c /path/to/config.toml
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `-c, --config` | Path to config file (default: `config.toml`) |
| `-d, --download-dir` | Override download directory from config |
| `-m, --manifest-url` | Override manifest URL from config |
| `-j, --concurrency` | Number of concurrent downloads |
| `-r, --run-integrity` | Run integrity check on existing files before downloading |
| `-i, --integrity-retries` | Number of times to retry downloading files that fail integrity checks |
| `--concurrent-validations` | Number of concurrent parquet validations (default: CPU count) |
| `-n, --dry-run` | Preview what would be downloaded without downloading |
| `-f, --format` | Format version: v1 (JSON manifest) or v2 (XML listing with checksums) |
| `-v, --verbose` | Enable verbose output (DEBUG level) |
| `-q, --quiet` | Quiet mode (only warnings and errors) |
| `--log-file` | Write logs to file (always DEBUG level) |

## Configuration

Edit `config.toml` to customize behavior:

```toml
# URL to the manifest file (used for v1 format)
manifest_url = "https://export.sourcify.dev/manifest.json"

# Directory where files will be downloaded (flattened)
download_dir = "./downloads"

# Path to aria2c binary
aria2c_path = "aria2c"

# Number of concurrent downloads
concurrent_downloads = 5

# Verify parquet file integrity after download
integrity_check = true

# Number of times to retry downloading files that fail integrity checks
integrity_retry_count = 3

# Format version: "v1" (JSON manifest) or "v2" (XML listing with checksums)
format_version = "v2"

# V2-specific settings
# v2_listing_url = "https://export.sourcify.dev/"
# v2_prefix = "v2/"
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `manifest_url` | `https://export.sourcify.dev/manifest.json` | URL to fetch the manifest (v1 format) |
| `download_dir` | `./downloads` | Target directory for downloaded files |
| `aria2c_path` | `aria2c` | Path to aria2c binary |
| `concurrent_downloads` | `5` | Number of parallel downloads |
| `integrity_check` | `true` | Verify parquet file integrity after download |
| `integrity_retry_count` | `3` | Number of retries for corrupt file downloads |
| `concurrent_validations` | CPU count | Number of concurrent parquet validations |
| `format_version` | `v2` | Format version: `v1` (JSON manifest) or `v2` (XML listing) |
| `v2_listing_url` | `https://export.sourcify.dev/` | Base URL for v2 file listing |
| `v2_prefix` | `v2/` | Prefix filter for v2 files |

## Features

- **Resume support**: Interrupted downloads automatically resume from where they left off
- **Incremental sync**: V2 format uses checksums to detect and download only changed files
- **Skip existing files**: Already downloaded files are not re-downloaded
- **Flattened storage**: All files are saved to a single directory regardless of their original folder structure
- **Manifest refresh**: The file listing is re-fetched on each run to detect new files
- **Progress display**: Real-time download progress via aria2c's console output
- **Configurable**: All settings can be customized via config file or CLI
- **Integrity verification**: Validates parquet file metadata and schema after download, with automatic retry for corrupt files

## How It Works

1. Loads configuration from `config.toml` (or specified config file)
2. Fetches the file listing (v2: XML listing with checksums, v1: JSON manifest)
3. Compares against local files (v2: checksum-based, v1: existence-based)
4. Generates an aria2c input file with URLs and output filenames
5. Executes aria2c to download the files with resume capability
6. Verifies parquet file integrity (if enabled), retrying corrupt files

## License

MIT
