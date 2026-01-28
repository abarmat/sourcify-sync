# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run the application
uv run python main.py

# Run with CLI overrides
uv run python main.py -d /path/to/downloads -m https://example.com/manifest.json -j 10

# Run with verbose logging
uv run python main.py -v

# Run in quiet mode (warnings/errors only)
uv run python main.py -q

# Log to file
uv run python main.py --log-file sync.log

# Preview what would be downloaded (dry run)
uv run python main.py -n

# Run integrity check on existing files
uv run python main.py -r

# Check syntax
just check

# Run tests
just test

# Run tests with verbose output
just test-v

# Run tests and stop on first failure
just test-x
```

## Architecture

This is a Python CLI tool that downloads files from a remote manifest using aria2c.

**Flow:**
- v1: `main.py` → `config.py` (load settings) → `manifest.py` (fetch JSON) → `downloader.py` (verify & download)
- v2: `main.py` → `config.py` (load settings) → `manifest_v2.py` (fetch XML listing) → `downloader.py` (checksum verify & download)

| Module | Purpose |
|--------|---------|
| `main.py` | CLI entry point, argument parsing, orchestration |
| `config.py` | TOML config loading with CLI override support |
| `manifest.py` | Fetch and parse v1 manifest JSON from remote URL |
| `manifest_v2.py` | Fetch and parse v2 XML listing with checksums |
| `file_info.py` | FileInfo dataclass for v2 file metadata (key, size, etag) |
| `downloader.py` | Local file verification, aria2c execution with session support, parquet integrity checking |
| `logging_setup.py` | Logging configuration with verbosity levels and file output support |

**Key design decisions:**
- V2 format (default): uses checksums for incremental sync, only downloads changed files
- V1 format: trusts local files if size > 0 (no HEAD requests)
- aria2c session file (`{download_dir}/{version}/.aria2c-session`) persists state for resume across runs
- Files are flattened: `code/code_0_100000.parquet` → `code_0_100000.parquet`
- Downloads are segregated by format version: `downloads/v1/` or `downloads/v2/`
- Optional parquet integrity check validates metadata/schema and retries corrupt files

## Contributing

- When writing commit messages do it with a brief description. If there are many changes use at most 3 bullets.
- When creating a PR, do not include any message about attribution. Keep it clean.

## Requirements

- Python 3.13+
- aria2c (install via `brew install aria2` or `apt install aria2`)
- just (install via `brew install just` or `cargo install just`)
