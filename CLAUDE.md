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

**Flow:** `main.py` → `config.py` (load settings) → `manifest.py` (fetch JSON) → `downloader.py` (verify & download)

| Module | Purpose |
|--------|---------|
| `main.py` | CLI entry point, argument parsing, orchestration |
| `config.py` | TOML config loading with CLI override support |
| `manifest.py` | Fetch and parse manifest JSON from remote URL |
| `downloader.py` | Local file verification, aria2c execution with session support, parquet integrity checking |
| `logging_setup.py` | Logging configuration with verbosity levels and file output support |

**Key design decisions:**
- Trusts local files: if a file exists with size > 0, it's considered complete (no HEAD requests)
- aria2c session file (`{download_dir}/.aria2c-session`) persists state for resume across runs
- Files are flattened: `code/code_0_100000.parquet` → `code_0_100000.parquet`
- Base URL is auto-derived from manifest URL
- Optional parquet integrity check validates metadata/schema and retries corrupt files

## Contributing

- When writing commit messages do it with a brief description. If there are many changes use at most 3 bullets.
- When creating a PR, do not include any message about attribution. Keep it clean.

## Requirements

- Python 3.13+
- aria2c (install via `brew install aria2` or `apt install aria2`)
- just (install via `brew install just` or `cargo install just`)
