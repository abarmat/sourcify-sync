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

# Check syntax
uv run python -m py_compile main.py config.py manifest.py downloader.py
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
- aria2c (external binary, install via `brew install aria2` or `apt install aria2`)
