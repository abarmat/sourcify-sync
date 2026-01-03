# Run all tests
test *args:
    uv run pytest {{args}}

# Run tests with verbose output
test-v *args:
    uv run pytest -v {{args}}

# Run tests and stop on first failure
test-x *args:
    uv run pytest -x {{args}}

# Run tests with coverage
test-cov *args:
    uv run pytest --cov=. --cov-report=term-missing {{args}}

# Check Python syntax
check:
    uv run python -m py_compile main.py config.py manifest.py downloader.py logging_setup.py

# Run the application
run *args:
    uv run python main.py {{args}}
