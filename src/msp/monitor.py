"""Run tracking, alerting and the HTML run report.

* pipeline_runs / step_metrics tables: every run and step is recorded with
  timings and row counts, so "is the pipeline healthy?" is a SQL query.
* Alerts go to stdout and, if ALERT_WEBHOOK_URL is set, to a Teams/Slack
  incoming webhook (both accept {"text": ...}).
* A self-contained HTML report summarises each run for humans.
"""
from __future__ import annotations

import html
import json
import os
import time
import urllib.request
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import duckdb


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id      VARCHAR PRIMARY KEY,
            started_at  TIMESTAMP,
            finished_at TIMESTAMP,
            status      VARCHAR,   -- running | success | success_with_warnings | failed | crashed
            summary     VARCHAR
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS step_metrics (
            run_id   VARCHAR,
            step     VARCHAR,
            seconds  DOUBLE,
            metrics  JSON
        )""")


def start_run(con, run_id: str) -> None:
    ensure_tables(con)
    con.execute("INSERT INTO pipeline_runs VALUES (?, ?, NULL, 'running', NULL)", [run_id, datetime.now()])


def finish_run(con, run_id: str, status: str, summary: str) -> None:
    con.execute("UPDATE pipeline_runs SET finished_at = ?, status = ?, summary = ? WHERE run_id = ?",
                [datetime.now(), status, summary, run_id])


@contextmanager
def step(con, run_id: str, name: str, log=print):
    """Time a pipeline step and store whatever metrics it reports."""
    metrics: dict = {}
    t0 = time.perf_counter()
    log(f"▶ {name}")
    yield metrics
    secs = time.perf_counter() - t0
    con.execute("INSERT INTO step_metrics VALUES (?, ?, ?, ?)", [run_id, name, secs, json.dumps(metrics, default=str)])
    log(f"  ✓ {name} ({secs:.2f}s) {metrics}")


def alert(level: str, message: str, log=print) -> None:
    icon = {"error": "🔴", "warn": "🟡", "info": "🟢"}.get(level, "•")
    text = f"{icon} [media-spend-platform] {message}"
    log(text)
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        try:
            req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
        except Exception as exc:  # alerting must never break the pipeline
            log(f"  (webhook delivery failed: {exc})")


# ------------------------------------------------------------------ report --
CSS = """
:root{--bg:#fafaf8;--fg:#1d1d1b;--muted:#6b6b66;--line:#e4e3de;--card:#fff;
--pass:#1f7a4d;--warn:#a86400;--fail:#b3261e;--accent:#2d4a7a}
@media (prefers-color-scheme:dark){:root{--bg:#161615;--fg:#ecebe6;--muted:#a3a29c;
--line:#2e2e2b;--card:#1e1e1c;--pass:#5cc08f;--warn:#e6a54a;--fail:#f2786e;--accent:#8fb0e6}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif}
main{max-width:1080px;margin:0 auto;padding:32px 16px 64px}
h1{font-size:24px;margin:0 0 4px}h2{font-size:16px;margin:36px 0 10px;text-transform:uppercase;
letter-spacing:.06em;color:var(--muted)}.sub{color:var(--muted);margin:0 0 24px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px}
.kpi b{display:block;font-size:22px;font-variant-numeric:tabular-nums}.kpi span{color:var(--muted);font-size:13px}
.wrap{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:10px}
table{border-collapse:collapse;width:100%;font-size:13.5px}th,td{padding:8px 12px;text-align:left;
border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted);font-weight:600}
tr:last-child td{border-bottom:0}td.n{text-align:right;font-variant-numeric:tabular-nums}
.pill{display:inline-block;padding:1px 8px;border-radius:99px;font-size:12px;font-weight:600;border:1px solid}
.pass{color:var(--pass)}.warn{color:var(--warn)}.fail,.failed,.crashed,.rejected{color:var(--fail)}
.success{color:var(--pass)}.success_with_warnings{color:var(--warn)}.loaded{color:var(--pass)}
code{font-size:12px;color:var(--muted);word-break:break-all}
"""


def _table(headers, rows, numeric=()):
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = []
    for r in rows:
        cells = []
        for i, v in enumerate(r):
            cls = ' class="n"' if i in numeric else ""
            if isinstance(v, (float, Decimal)):
                v = f"{v:,.2f}"
            elif isinstance(v, int):
                v = f"{v:,}"
            if headers[i] in ("status",):
                s = html.escape(str(v))
                cells.append(f'<td><span class="pill {s}">{s}</span></td>')
            else:
                cells.append(f"<td{cls}>{html.escape('' if v is None else str(v))}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f'<div class="wrap"><table><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def write_report(con, run_id: str, reports_dir: Path) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    run = con.execute("SELECT status, started_at, finished_at, summary FROM pipeline_runs WHERE run_id = ?",
                      [run_id]).fetchone()
    kpi = con.execute("""
        SELECT (SELECT count(*) FROM bronze_raw),
               (SELECT count(*) FROM quarantine_media_rows),
               (SELECT count(*) FROM fact_media_spend),
               (SELECT round(sum(spend_eur), 0) FROM fact_media_spend),
               (SELECT count(*) FROM ingestion_log WHERE status = 'loaded')
    """).fetchone()

    parts = [f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pipeline Run Report</title><style>{CSS}</style></head><body><main>
<h1>Media spend pipeline · run {html.escape(run_id)}</h1>
<p class="sub"><span class="pill {run[0]}">{run[0]}</span> &nbsp;started {run[1]:%Y-%m-%d %H:%M:%S}
 · {html.escape(run[3] or '')}</p>
<div class="kpis">
<div class="kpi"><b>{kpi[4]:,}</b><span>files loaded (all time)</span></div>
<div class="kpi"><b>{kpi[0]:,}</b><span>bronze rows</span></div>
<div class="kpi"><b>{kpi[1]:,}</b><span>quarantined rows</span></div>
<div class="kpi"><b>{kpi[2]:,}</b><span>fact rows</span></div>
<div class="kpi"><b>€{(kpi[3] or 0):,.0f}</b><span>total spend</span></div>
</div>"""]

    dq = con.execute("""SELECT check_name, severity, status, details FROM dq_results
                        WHERE run_id = ? ORDER BY CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, check_name""",
                     [run_id]).fetchall()
    parts.append("<h2>Data quality checks</h2>" + _table(["check", "severity", "status", "details"], dq))

    steps = con.execute("SELECT step, round(seconds, 3), metrics FROM step_metrics WHERE run_id = ?", [run_id]).fetchall()
    parts.append("<h2>Steps</h2>" + _table(["step", "seconds", "metrics"], steps, numeric=(1,)))

    files = con.execute("""SELECT file_id, source, file_name, status, row_count, coalesce(message, '')
                           FROM ingestion_log WHERE run_id = ? ORDER BY file_id""", [run_id]).fetchall()
    parts.append("<h2>Files ingested this run</h2>" + (
        _table(["id", "source", "file", "status", "rows", "message"], files, numeric=(0, 4))
        if files else '<p class="sub">No new files — nothing was reloaded (idempotent run).</p>'))

    q = con.execute("""SELECT reject_reason, source, count(*) FROM quarantine_media_rows
                       GROUP BY ALL ORDER BY 3 DESC""").fetchall()
    parts.append("<h2>Quarantine by reason</h2>" + _table(["reason", "source", "rows"], q, numeric=(2,)))

    spend = con.execute("""SELECT source, channel, sum(impressions)::BIGINT, round(sum(spend_eur), 2)
                           FROM fact_media_spend GROUP BY ALL ORDER BY 1, 4 DESC""").fetchall()
    parts.append("<h2>Spend by agency and channel (EUR)</h2>" +
                 _table(["source", "channel", "impressions", "spend_eur"], spend, numeric=(2, 3)))

    hist = con.execute("""SELECT run_id, strftime(started_at, '%Y-%m-%d %H:%M:%S'), status, summary
                          FROM pipeline_runs ORDER BY started_at DESC LIMIT 10""").fetchall()
    parts.append("<h2>Recent runs</h2>" + _table(["run", "started", "status", "summary"], hist))
    parts.append("</main></body></html>")

    out = reports_dir / f"run_{run_id}.html"
    out.write_text("\n".join(parts))
    (reports_dir / "latest.html").write_text(out.read_text())
    return out
