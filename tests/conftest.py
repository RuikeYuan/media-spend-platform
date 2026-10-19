import duckdb
import pytest

from msp.generate import generate
from msp.pipeline import run
from msp.settings import Settings


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Isolated data directory per test; config and SQL come from the repo."""
    return Settings(data_dir=tmp_path / "data")


@pytest.fixture
def loaded(settings):
    """Settings after one full pipeline run on the standard sample delivery."""
    generate(settings.landing_dir)
    outcome = run(settings, log=lambda *_: None)
    return settings, outcome


def query(settings: Settings, sql: str):
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()
