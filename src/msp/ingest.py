"""Bronze layer: land agency files exactly as delivered.

Design choices worth talking about:
  * Idempotent: each file is identified by its SHA-256. Re-running the pipeline,
    or an agency re-sending the identical file, never double-loads data.
  * Schema-agnostic storage: every row is stored as a JSON payload, so a new
    column from an agency never breaks the load. The contract decides what we read.
  * Schema drift detection: a file missing a column the contract requires is
    rejected (and alerted on) instead of silently producing NULLs downstream.
  * Lineage: every bronze row keeps file_id, file_name and row_num, so any number
    in a client report can be traced back to the exact line an agency sent.
"""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

import duckdb


@dataclass
class IngestResult:
    new_files: int = 0
    skipped_files: int = 0
    rejected_files: list[tuple[str, str]] = field(default_factory=list)
    rows_loaded: int = 0


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_file_id START 1")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            file_id     INTEGER PRIMARY KEY,
            run_id      VARCHAR,
            source      VARCHAR,
            file_name   VARCHAR,
            file_hash   VARCHAR,
            status      VARCHAR,      -- loaded | rejected
            message     VARCHAR,
            row_count   INTEGER,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_raw (
            file_id     INTEGER,
            source      VARCHAR,
            file_name   VARCHAR,
            row_num     INTEGER,
            payload     JSON,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )""")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def read_file(path: Path, contract: dict) -> tuple[list[str], list[dict]]:
    """Return (columns seen, rows as dicts of strings)."""
    if contract["format"] == "csv":
        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=contract.get("delimiter", ","))
            rows = list(reader)
            return list(reader.fieldnames or []), rows
    if contract["format"] == "jsonl":
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        cols = sorted({k for r in rows for k in r})
        return cols, rows
    raise ValueError(f"Unsupported format {contract['format']!r}")


def check_schema(columns: list[str], contract: dict) -> str | None:
    required = set(contract["columns"].values())
    missing = sorted(required - set(columns))
    if missing:
        return f"missing required columns: {missing}"
    return None


def ingest(con: duckdb.DuckDBPyConnection, contracts: dict, landing_dir: Path, run_id: str) -> IngestResult:
    ensure_tables(con)
    result = IngestResult()
    seen = {h for (h,) in con.execute("SELECT file_hash FROM ingestion_log").fetchall()}

    for source, contract in contracts.items():
        folder = landing_dir / source
        if not folder.exists():
            continue
        for path in sorted(folder.glob(contract["file_pattern"])):
            file_hash = _sha256(path)
            if file_hash in seen:
                result.skipped_files += 1
                continue
            seen.add(file_hash)
            file_id = con.execute("SELECT nextval('seq_file_id')").fetchone()[0]

            try:
                columns, rows = read_file(path, contract)
                problem = check_schema(columns, contract)
            except Exception as exc:  # unreadable file
                rows, problem = [], f"unreadable: {exc}"

            if problem:
                con.execute(
                    "INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, 'rejected', ?, 0, current_timestamp)",
                    [file_id, run_id, source, path.name, file_hash, problem])
                result.rejected_files.append((path.name, problem))
                continue

            # One transaction per file: rows and log entry land together or not at all,
            # so a crash mid-file can never leave a half-loaded file marked as done.
            con.begin()
            try:
                con.executemany(
                    "INSERT INTO bronze_raw (file_id, source, file_name, row_num, payload) VALUES (?, ?, ?, ?, ?)",
                    [(file_id, source, path.name, i, json.dumps(r)) for i, r in enumerate(rows, start=1)])
                con.execute(
                    "INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, 'loaded', NULL, ?, current_timestamp)",
                    [file_id, run_id, source, path.name, file_hash, len(rows)])
                con.commit()
            except Exception:
                con.rollback()
                raise
            result.new_files += 1
            result.rows_loaded += len(rows)
    return result
