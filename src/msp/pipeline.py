"""Orchestrator and CLI.

    python -m msp.pipeline generate [--late]   # write sample agency files
    python -m msp.pipeline run [--no-fail]     # bronze -> silver -> gold -> checks -> report
    python -m msp.pipeline reset               # delete warehouse, landing files and reports

Exit codes: 0 success (warnings allowed), 1 an error-severity check failed,
2 the pipeline crashed. Non-zero codes are what a scheduler (Airflow, ADF,
Databricks Workflows) or CI uses to stop downstream jobs.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from . import ingest, monitor, quality, transform
from .generate import generate
from .settings import Settings


@dataclass
class RunOutcome:
    run_id: str
    status: str
    checks: list = field(default_factory=list)
    report: Path | None = None

    @property
    def exit_code(self) -> int:
        return {"success": 0, "success_with_warnings": 0, "failed": 1}.get(self.status, 2)


def run(settings: Settings | None = None, log=print) -> RunOutcome:
    settings = settings or Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:8]
    contracts = settings.contracts()
    con = duckdb.connect(str(settings.warehouse_path))
    monitor.start_run(con, run_id)
    log(f"Run {run_id} → {settings.warehouse_path}")

    try:
        with monitor.step(con, run_id, "load_seeds", log):
            transform.load_seeds(con, settings.config_dir, contracts)

        with monitor.step(con, run_id, "bronze_ingest", log) as m:
            res = ingest.ingest(con, contracts, settings.landing_dir, run_id)
            m.update(new_files=res.new_files, skipped_files=res.skipped_files,
                     rejected_files=len(res.rejected_files), rows_loaded=res.rows_loaded)
        for name, problem in res.rejected_files:
            monitor.alert("error", f"File rejected at ingest: {name} — {problem}", log)

        with monitor.step(con, run_id, "silver_build", log) as m:
            s = transform.build_silver(con, contracts)
            m.update(staged=s.staged_rows, quarantined=s.quarantined_rows,
                     superseded_or_duplicate=s.superseded_rows, silver=s.silver_rows)

        with monitor.step(con, run_id, "gold_build", log) as m:
            m.update(transform.build_gold(con, settings.sql_dir))

        with monitor.step(con, run_id, "quality_checks", log) as m:
            checks = quality.run_checks(con, settings.quality_checks(), run_id)
            m.update(passed=sum(c.passed for c in checks),
                     warnings=sum(c.status == "warn" for c in checks),
                     failures=sum(c.status == "fail" for c in checks))
    except Exception as exc:
        monitor.finish_run(con, run_id, "crashed", f"{type(exc).__name__}: {exc}")
        monitor.alert("error", f"Run {run_id} crashed: {exc}", log)
        con.close()
        raise

    failures = [c for c in checks if c.status == "fail"]
    warnings = [c for c in checks if c.status == "warn"]
    for c in failures:
        monitor.alert("error", f"DQ check FAILED: {c.name} — {c.details}", log)
    for c in warnings:
        monitor.alert("warn", f"DQ check warning: {c.name} — {c.details}", log)

    status = "failed" if failures else ("success_with_warnings" if warnings or res.rejected_files else "success")
    summary = (f"{res.new_files} new files, {s.silver_rows} silver rows, "
               f"{s.quarantined_rows} quarantined, {len(failures)} failed / {len(warnings)} warned checks")
    monitor.finish_run(con, run_id, status, summary)
    report = monitor.write_report(con, run_id, settings.reports_dir)
    con.close()

    log(f"\nRun {run_id}: {status.upper()} — {summary}")
    log(f"Report: {report}")
    return RunOutcome(run_id, status, checks, report)


def reset(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    for p in (settings.landing_dir, settings.reports_dir):
        shutil.rmtree(p, ignore_errors=True)
    for suffix in ("", ".wal"):
        Path(str(settings.warehouse_path) + suffix).unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="msp", description="Media spend data platform")
    sub = parser.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="write sample agency delivery files")
    g.add_argument("--late", action="store_true", help="also deliver agency_charlie's late final file")
    r = sub.add_parser("run", help="run the full pipeline")
    r.add_argument("--no-fail", action="store_true", help="exit 0 even if error-level checks fail")
    sub.add_parser("reset", help="delete all generated data")
    sub.add_parser("demo", help="write the sample delivery (incl. late file) if missing, then run")
    args = parser.parse_args(argv)

    settings = Settings()
    if args.cmd == "demo":
        # Used by the scheduled cloud job: self-contained, no external landing zone needed.
        # Generating is idempotent (same files, same hashes), so reruns load nothing new.
        generate(settings.landing_dir, late=True)
        print(f"Sample delivery ready in {settings.landing_dir}")
    if args.cmd == "generate":
        paths = generate(settings.landing_dir, late=args.late)
        print(f"Wrote {len(paths)} files to {settings.landing_dir}")
        return 0
    if args.cmd == "reset":
        reset(settings)
        print("Reset done.")
        return 0
    try:
        outcome = run(settings)
    except Exception:
        return 2
    return 0 if getattr(args, "no_fail", False) else outcome.exit_code


if __name__ == "__main__":
    sys.exit(main())
