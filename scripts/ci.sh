#!/usr/bin/env bash
# Local CI: the same gates a hosted CI would run, so nothing broken gets pushed.
#   1. lint   2. unit + end-to-end tests   3. full pipeline run on fresh sample data
# Runs in a throwaway data directory so it never touches your working data.
set -euo pipefail

cd "$(dirname "$0")/.."
export PYTHONPATH=src
PY="${PY:-python3}"

step() { printf '\n\033[1m▶ %s\033[0m\n' "$1"; }

step "Lint (ruff)"
"$PY" -m ruff check src tests

step "Tests (pytest)"
"$PY" -m pytest -q

step "Pipeline smoke run (fails on any error-level data quality check)"
CI_DATA="$(mktemp -d)"
trap 'rm -rf "$CI_DATA"' EXIT
"$PY" - "$CI_DATA" <<'EOF'
import sys
from pathlib import Path
from msp.generate import generate
from msp.pipeline import run
from msp.settings import Settings

settings = Settings(data_dir=Path(sys.argv[1]))
generate(settings.landing_dir, late=True)
outcome = run(settings, log=lambda *_: None)
failed = [c.name for c in outcome.checks if c.status == "fail"]
print(f"status={outcome.status} failed_checks={failed}")
sys.exit(outcome.exit_code)
EOF

printf '\n\033[32m✓ CI passed\033[0m\n'
