# Sourcify Sync

Download files from the Sourcify export manifest using aria2c.

## Requirements

- Python 3.13+
- [aria2c](https://aria2.github.io/) - high-performance download utility

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

## Configuration

Edit `config.toml` to customize behavior:

```toml
# URL to the manifest file
manifest_url = "https://export.sourcify.dev/manifest.json"

# Directory where files will be downloaded (flattened)
download_dir = "./downloads"

# Path to aria2c binary
aria2c_path = "aria2c"

# Number of concurrent downloads
concurrent_downloads = 5
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `manifest_url` | `https://export.sourcify.dev/manifest.json` | URL to fetch the manifest |
| `download_dir` | `./downloads` | Target directory for downloaded files |
| `aria2c_path` | `aria2c` | Path to aria2c binary |
| `concurrent_downloads` | `5` | Number of parallel downloads |

## Features

- **Resume support**: Interrupted downloads automatically resume from where they left off
- **Skip existing files**: Already downloaded files are not re-downloaded
- **Flattened storage**: All files are saved to a single directory regardless of their original folder structure
- **Manifest refresh**: The manifest is re-fetched on each run to detect new files
- **Progress display**: Real-time download progress via aria2c's console output
- **Configurable**: All settings can be customized via config file or CLI

## How It Works

1. Loads configuration from `config.toml` (or specified config file)
2. Fetches the manifest JSON from the configured URL
3. Extracts all file paths from the manifest
4. Filters out files that already exist in the download directory
5. Generates an aria2c input file with URLs and output filenames
6. Executes aria2c to download the files with resume capability

## License

MIT
