from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _path(env: str, default: Path) -> Path:
    """Paths are overridable via env vars so the same code runs locally and in a container."""
    return Path(os.environ[env]) if os.environ.get(env) else default


@dataclass(frozen=True)
class Settings:
    """All paths the pipeline touches. `data_dir` is swapped out in tests."""

    config_dir: Path = field(default_factory=lambda: _path("MSP_CONFIG_DIR", PROJECT_ROOT / "config"))
    sql_dir: Path = field(default_factory=lambda: _path("MSP_SQL_DIR", PROJECT_ROOT / "sql"))
    data_dir: Path = field(default_factory=lambda: _path("MSP_DATA_DIR", PROJECT_ROOT / "data"))

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"

    def contracts(self) -> dict:
        return yaml.safe_load((self.config_dir / "sources.yml").read_text())

    def quality_checks(self) -> list[dict]:
        return yaml.safe_load((self.config_dir / "quality_checks.yml").read_text())["checks"]
