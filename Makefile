test:
	@pytest

fix:
	@ruff check --fix

check-fix:
	@ruff check

format:
	@ruff format

check-format:
	@ruff format --check

.PHONY: test fix check-fix format check-format
