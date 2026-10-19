PY ?= python3
export PYTHONPATH := src

.PHONY: install demo generate run late test lint reset

install:
	$(PY) -m pip install -e ".[dev]"

## Full story in one command: first delivery -> rerun (idempotent) -> late file arrives
demo: reset
	$(PY) -m msp.pipeline generate
	$(PY) -m msp.pipeline run
	@echo "\n--- rerun with no new files (should load nothing) ---"
	$(PY) -m msp.pipeline run
	@echo "\n--- agency_charlie delivers its late file ---"
	$(PY) -m msp.pipeline generate --late
	$(PY) -m msp.pipeline run

generate:
	$(PY) -m msp.pipeline generate

run:
	$(PY) -m msp.pipeline run

late:
	$(PY) -m msp.pipeline generate --late

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src tests

reset:
	$(PY) -m msp.pipeline reset
