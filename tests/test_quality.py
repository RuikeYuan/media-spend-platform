"""Unit tests for the quality engine on a tiny in-memory table."""
import duckdb
import pytest

from msp.quality import run_checks


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("""CREATE TABLE t AS SELECT * FROM (VALUES
        (1, 'TV', 100, 5), (2, 'SOCIAL', 50, 60), (2, NULL, 10, 1)) v(id, channel, imps, clicks)""")
    c.execute("CREATE TABLE ref AS SELECT 1 AS id")
    return c


@pytest.mark.parametrize("check, expected_status, expected_observed", [
    ({"type": "not_null", "table": "t", "columns": ["channel"]}, "fail", 1),
    ({"type": "unique", "table": "t", "columns": ["id"]}, "fail", 1),
    ({"type": "accepted_values", "table": "t", "column": "channel", "values": ["TV", "SOCIAL"]}, "pass", 0),
    ({"type": "expression", "table": "t", "expression": "clicks <= imps", "severity": "warn"}, "warn", 1),
    ({"type": "relationship", "table": "t", "column": "id", "to_table": "ref", "to_column": "id"}, "fail", 2),
    ({"type": "reconcile", "left_sql": "SELECT 10.0", "right_sql": "SELECT 10.004", "tolerance": 0.01}, "pass", None),
    ({"type": "metric", "sql": "SELECT count(*) FROM t", "max": 2}, "fail", 3),
])
def test_check_types(con, check, expected_status, expected_observed):
    [result] = run_checks(con, [{"name": "c", **check}], run_id="test")
    assert result.status == expected_status
    if expected_observed is not None:
        assert result.observed == expected_observed


def test_broken_check_fails_instead_of_passing(con):
    [result] = run_checks(con, [{"name": "c", "type": "metric", "sql": "SELECT * FROM missing", "max": 0}], "test")
    assert result.status == "fail" and "errored" in result.details


def test_results_are_persisted(con):
    run_checks(con, [{"name": "c", "type": "metric", "sql": "SELECT 1", "max": 5}], "run42")
    assert con.execute("SELECT run_id, status FROM dq_results").fetchall() == [("run42", "pass")]
