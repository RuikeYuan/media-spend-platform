"""Config-driven data quality engine (a small, dependency-free Great Expectations).

Each check returns a CheckResult; results are persisted to `dq_results` so data
quality is itself queryable over time (trend of failures, flaky feeds, etc.).

The engine only needs `con.execute(sql)` returning an object with fetchone() /
fetchall(), so the same checks run on DuckDB and on Spark (see msp_spark). A check
can carry an engine-specific `spark_sql` when the dialects differ.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # duckdb is only needed by the DuckDB pipeline
    import duckdb


@dataclass
class CheckResult:
    name: str
    type: str
    severity: str
    passed: bool
    observed: float | None
    details: str

    @property
    def status(self) -> str:
        if self.passed:
            return "pass"
        return "fail" if self.severity == "error" else "warn"


def _scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


def _not_null(con, c):
    preds = " OR ".join(f"{col} IS NULL" for col in c["columns"])
    bad = _scalar(con, f"SELECT count(*) FROM {c['table']} WHERE {preds}")
    return bad == 0, bad, f"{bad} rows with NULL in {c['columns']}"


def _unique(con, c):
    cols = ", ".join(c["columns"])
    bad = _scalar(con, f"SELECT count(*) FROM (SELECT {cols} FROM {c['table']} GROUP BY ALL HAVING count(*) > 1)")
    return bad == 0, bad, f"{bad} duplicated keys on ({cols})"


def _accepted_values(con, c):
    values = ", ".join("'" + v.replace("'", "''") + "'" for v in c["values"])
    rows = con.execute(
        f"SELECT DISTINCT {c['column']} FROM {c['table']} WHERE {c['column']} NOT IN ({values})").fetchall()
    unexpected = [r[0] for r in rows]
    return not unexpected, len(unexpected), f"unexpected values: {unexpected}" if unexpected else "all values accepted"


def _expression(con, c):
    bad = _scalar(con, f"SELECT count(*) FROM {c['table']} WHERE NOT ({c['expression']})")
    return bad == 0, bad, f"{bad} rows violate `{c['expression']}`"


def _relationship(con, c):
    bad = _scalar(con, f"""
        SELECT count(*) FROM {c['table']} t
        WHERE t.{c['column']} IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {c['to_table']} r WHERE r.{c['to_column']} = t.{c['column']})""")
    return bad == 0, bad, f"{bad} orphan rows ({c['table']}.{c['column']} -> {c['to_table']}.{c['to_column']})"


def _reconcile(con, c):
    left = _scalar(con, c["left_sql"]) or 0
    right = _scalar(con, c["right_sql"]) or 0
    diff = abs(float(left) - float(right))
    return diff <= c.get("tolerance", 0), diff, f"left={float(left):,.2f} right={float(right):,.2f} diff={diff:,.4f}"


def _metric(con, c):
    value = _scalar(con, c["sql"])
    value = float(value) if value is not None else 0.0
    ok = True
    bounds = []
    if "max" in c:
        ok &= value <= c["max"]
        bounds.append(f"<= {c['max']}")
    if "min" in c:
        ok &= value >= c["min"]
        bounds.append(f">= {c['min']}")
    desc = c.get("description", "")
    return ok, value, f"observed {value:g} (expected {' and '.join(bounds)}). {desc}".strip()


CHECKS = {
    "not_null": _not_null,
    "unique": _unique,
    "accepted_values": _accepted_values,
    "expression": _expression,
    "relationship": _relationship,
    "reconcile": _reconcile,
    "metric": _metric,
}


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id     VARCHAR,
            check_name VARCHAR,
            check_type VARCHAR,
            severity   VARCHAR,
            status     VARCHAR,
            observed   DOUBLE,
            details    VARCHAR,
            checked_at TIMESTAMP DEFAULT current_timestamp
        )""")


def evaluate(con, checks: list[dict], engine: str = "duckdb") -> list[CheckResult]:
    """Run every check and return the results, without persisting anything."""
    results = []
    for c in checks:
        fn = CHECKS.get(c["type"])
        if fn is None:
            raise ValueError(f"Unknown check type {c['type']!r} in {c['name']}")
        if engine == "spark" and "spark_sql" in c:
            c = {**c, "sql": c["spark_sql"]}
        try:
            passed, observed, details = fn(con, c)
        except Exception as exc:  # a broken check must fail loudly, not pass silently
            passed, observed, details = False, None, f"check errored: {exc}"
        results.append(CheckResult(c["name"], c["type"], c.get("severity", "error"), bool(passed),
                                   None if observed is None else float(observed), details))
    return results


def run_checks(con: duckdb.DuckDBPyConnection, checks: list[dict], run_id: str) -> list[CheckResult]:
    ensure_tables(con)
    results = evaluate(con, checks)
    for r in results:
        con.execute("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp)",
                    [run_id, r.name, r.type, r.severity, r.status, r.observed, r.details])
    return results
