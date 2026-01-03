---
description: Analyze source code and update documentation to reflect current implementation
allowed-tools: Read, Grep, Glob, Edit
---

# Update Project Documentation

Analyze the Python source code and update documentation files to accurately reflect the current implementation.

## Source Files (Source of Truth)

Read all Python source files and configuration:
@**/*.py
@*.toml
@pyproject.toml

## Documentation Files (To Update)

@README.md
@CLAUDE.md

## Analysis Steps

### Step 1: Extract CLI Arguments

Search for `argparse` usage and `add_argument()` calls. For each argument extract:
- Short flag (e.g., `-m`)
- Long flag (e.g., `--manifest-url`)
- Help text
- Default value

### Step 2: Extract Configuration Options

Find configuration defaults (look for `DEFAULTS` dict or similar patterns). For each option extract:
- Option name
- Default value
- Description (from comments or TOML files)

### Step 3: Verify Architecture Descriptions

Check `CLAUDE.md` architectural statements against actual implementation:
- How file verification works
- Module purposes and flow
- Key design decisions

### Step 4: Compare and Identify Gaps

Compare extracted information against documentation:
- CLI options → README.md "Command Line Options" table
- Config options → README.md "Configuration Options" table
- Architecture → CLAUDE.md module descriptions and design decisions

## Update Guidelines

1. **Preserve existing style** - Match formatting, voice, and table structure
2. **Be precise** - Use exact flag names, defaults, and descriptions from code
3. **Minimal changes** - Only add/update what is missing or incorrect
4. **Keep tables aligned** - Maintain markdown table formatting

## Output

After analysis:
1. Update README.md with any missing CLI options and config options
2. Update CLAUDE.md with any incorrect architectural descriptions
3. Report what was changed and why
