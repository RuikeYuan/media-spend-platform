"""The Spark pipeline must produce exactly what the DuckDB pipeline produces.

Skipped when PySpark is not installed (it needs Java): `pip install -e ".[spark]"`.
"""
import pytest

pyspark = pytest.importorskip("pyspark")

from msp.generate import generate  # noqa: E402
from msp.pipeline import run as run_duckdb  # noqa: E402
from msp.settings import Settings  # noqa: E402
from msp_spark.pipeline import SparkEnv  # noqa: E402
from msp_spark.pipeline import run as run_spark  # noqa: E402

QUIET = {"log": lambda *_: None}


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    import os
    os.chdir(tmp_path_factory.mktemp("spark-warehouse"))  # keep spark-warehouse/ out of the repo
    e = SparkEnv.create(schema="msp_test")
    yield e
    e.spark.sql("DROP SCHEMA IF EXISTS msp_test CASCADE")


@pytest.fixture(scope="module")
def results(env, tmp_path_factory):
    settings = Settings(data_dir=tmp_path_factory.mktemp("data"))
    generate(settings.landing_dir, late=True)
    duck = run_duckdb(settings, **QUIET)
    spark = run_spark(settings, env, **QUIET)
    return settings, duck, spark, env


def test_same_check_outcomes_as_duckdb(results):
    _, duck, spark, _ = results
    assert {c.name: c.status for c in spark.checks} == {c.name: c.status for c in duck.checks}
    assert spark.status == duck.status == "success_with_warnings"


def test_layer_counts(results):
    *_, spark, _ = results
    assert spark.metrics["silver"] == {"staged_rows": 683, "quarantined_rows": 4,
                                       "superseded_rows": 51, "silver_rows": 628}
    assert spark.metrics["gold"]["fact_media_spend"] == 628


def test_total_spend_matches_duckdb(results):
    import duckdb
    settings, _, _, env = results
    duck_total = duckdb.connect(str(settings.warehouse_path), read_only=True) \
        .execute("SELECT sum(spend_eur) FROM fact_media_spend").fetchone()[0]
    spark_total = env.spark.sql("SELECT sum(spend_eur) FROM fact_media_spend").collect()[0][0]
    assert spark_total == duck_total


def test_quarantine_reasons(results):
    *_, env = results
    reasons = sorted(r[0] for r in env.spark.sql("SELECT reject_reason FROM quarantine_media_rows").collect())
    assert reasons == ["invalid_date", "missing_campaign_id", "negative_metric", "unmapped_channel:radio"]


def test_restatement_wins(results):
    *_, env = results
    files = env.spark.sql("""
        SELECT DISTINCT loaded_file_name FROM silver_media_spend
        WHERE source = 'agency_alpha' AND activity_date BETWEEN '2026-09-08' AND '2026-09-14'""").collect()
    assert [r[0] for r in files] == ["alpha_2026-09-08_2026-09-14_v2.csv"]


def test_rerun_loads_nothing_new(results):
    settings, _, _, env = results
    again = run_spark(settings, env, **QUIET)
    assert again.metrics["bronze"]["new_files"] == 0
    assert again.metrics["silver"]["silver_rows"] == 628
