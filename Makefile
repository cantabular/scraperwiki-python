RUN := uv run

test:
	@$(RUN) pytest

fix:
	@$(RUN) ruff check --fix

check-fix:
	@$(RUN) ruff check

format:
	@$(RUN) ruff format

check-format:
	@$(RUN) ruff format --check

.PHONY: test fix check-fix format check-format
