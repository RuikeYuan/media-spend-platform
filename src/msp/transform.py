"""Silver and gold layers.

Silver = conform every agency feed to one canonical schema, convert currency,
quarantine rows that break hard rules, and resolve duplicates/restatements.
Gold   = dimensional model (star schema) + a reporting mart, defined in sql/gold.

Silver and gold are rebuilt from bronze on every run. At this volume that is
the simplest way to stay correct (restatements, late files and contract changes
are all handled by re-deriving). At production scale the same logic becomes an
incremental MERGE on the affected (source, date) partitions.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

NATURAL_KEY = "source, activity_date, campaign_id, channel, publisher"


@dataclass
class SilverResult:
    staged_rows: int
    quarantined_rows: int
    superseded_rows: int
    silver_rows: int


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _json_field(column: str) -> str:
    key = column.replace('"', '\\"')
    return f"json_extract_string(payload, {_sql_str('$.' + chr(34) + key + chr(34))})"


def _number(expr: str, decimal_separator: str, sql_type: str = "DOUBLE") -> str:
    if decimal_separator == ",":
        expr = f"replace(replace(trim({expr}), '.', ''), ',', '.')"
    else:
        expr = f"replace(trim({expr}), ',', '')"
    return f"TRY_CAST({expr} AS {sql_type})"


def staged_select(source: str, contract: dict) -> str:
    """SELECT that maps one source's bronze payloads onto the canonical schema."""
    cols = contract["columns"]
    dec = contract.get("decimal_separator", ".")
    if "currency" in cols:
        currency = f"upper(trim({_json_field(cols['currency'])}))"
    else:
        currency = _sql_str(contract["currency"])
    return f"""
    SELECT
        source,
        file_id,
        file_name,
        row_num,
        payload,
        TRY_CAST(try_strptime(trim({_json_field(cols['activity_date'])}), {_sql_str(contract['date_format'])}) AS DATE) AS activity_date,
        nullif(trim({_json_field(cols['campaign_id'])}), '')            AS campaign_id,
        lower(trim({_json_field(cols['channel'])}))                       AS channel_raw,
        trim({_json_field(cols['publisher'])})                            AS publisher,
        TRY_CAST({_number(_json_field(cols['impressions']), dec)} AS BIGINT) AS impressions,
        TRY_CAST({_number(_json_field(cols['clicks']), dec)} AS BIGINT)      AS clicks,
        {_number(_json_field(cols['spend']), dec, 'DECIMAL(18,2)')}       AS spend_local,
        {currency}                                                        AS currency
    FROM bronze_raw
    WHERE source = {_sql_str(source)}
      AND file_id IN (SELECT file_id FROM ingestion_log WHERE status = 'loaded')"""


def load_seeds(con: duckdb.DuckDBPyConnection, config_dir: Path, contracts: dict) -> None:
    seeds = config_dir / "seeds"
    campaigns_csv = _sql_str(str(seeds / "campaigns.csv"))
    con.execute(f"CREATE OR REPLACE TABLE seed_campaigns AS SELECT * FROM read_csv({campaigns_csv}, header=true, all_varchar=true)")
    con.execute(f"""CREATE OR REPLACE TABLE seed_fx_rates AS
        SELECT currency, CAST(month AS DATE) AS month, CAST(rate_to_eur AS DOUBLE) AS rate_to_eur
        FROM read_csv({_sql_str(str(seeds / 'fx_rates.csv'))}, header=true, all_varchar=true)""")
    con.execute("CREATE OR REPLACE TABLE map_channel (source VARCHAR, channel_raw VARCHAR, channel VARCHAR)")
    con.executemany("INSERT INTO map_channel VALUES (?, ?, ?)", [
        (source, raw.lower(), canonical)
        for source, c in contracts.items()
        for raw, canonical in c["channel_map"].items()])


def build_silver(con: duckdb.DuckDBPyConnection, contracts: dict) -> SilverResult:
    union = "\nUNION ALL\n".join(staged_select(s, c) for s, c in contracts.items())
    con.execute(f"CREATE OR REPLACE TABLE stg_media_rows AS {union}")

    # Attach canonical channel + FX and list every hard-rule violation per row.
    con.execute("""
        CREATE OR REPLACE TABLE stg_media_validated AS
        SELECT
            s.*,
            m.channel,
            fx.rate_to_eur,
            -- money stays DECIMAL end to end so totals reconcile to the cent
            CAST(round(s.spend_local * fx.rate_to_eur, 2) AS DECIMAL(18,2)) AS spend_eur,
            nullif(concat_ws(', ',
                CASE WHEN s.activity_date IS NULL THEN 'invalid_date' END,
                CASE WHEN s.campaign_id IS NULL THEN 'missing_campaign_id' END,
                CASE WHEN s.impressions IS NULL OR s.clicks IS NULL OR s.spend_local IS NULL
                     THEN 'non_numeric_metric' END,
                CASE WHEN s.impressions < 0 OR s.clicks < 0 OR s.spend_local < 0
                     THEN 'negative_metric' END,
                CASE WHEN m.channel IS NULL THEN 'unmapped_channel:' || coalesce(s.channel_raw, 'null') END,
                CASE WHEN s.activity_date IS NOT NULL AND fx.rate_to_eur IS NULL
                     THEN 'missing_fx_rate:' || coalesce(s.currency, 'null') END
            ), '') AS reject_reason
        FROM stg_media_rows s
        LEFT JOIN map_channel m
               ON m.source = s.source AND m.channel_raw = s.channel_raw
        LEFT JOIN seed_fx_rates fx
               ON fx.currency = s.currency AND fx.month = date_trunc('month', s.activity_date)
    """)

    con.execute("""
        CREATE OR REPLACE TABLE quarantine_media_rows AS
        SELECT source, file_id, file_name, row_num, reject_reason, payload
        FROM stg_media_validated
        WHERE reject_reason IS NOT NULL
        ORDER BY source, file_id, row_num
    """)

    # Latest delivery wins: a restatement (higher file_id) replaces earlier rows
    # for the same natural key; exact duplicates inside a file collapse to one.
    con.execute(f"""
        CREATE OR REPLACE TABLE silver_media_spend AS
        SELECT {NATURAL_KEY}, impressions, clicks, spend_local, currency, spend_eur,
               file_id AS loaded_file_id, file_name AS loaded_file_name, row_num AS loaded_row_num
        FROM (
            SELECT *, row_number() OVER (
                PARTITION BY {NATURAL_KEY} ORDER BY file_id DESC, row_num DESC) AS rn
            FROM stg_media_validated
            WHERE reject_reason IS NULL)
        WHERE rn = 1
        ORDER BY source, activity_date, campaign_id, channel, publisher
    """)

    staged = con.execute("SELECT count(*) FROM stg_media_rows").fetchone()[0]
    quarantined = con.execute("SELECT count(*) FROM quarantine_media_rows").fetchone()[0]
    silver = con.execute("SELECT count(*) FROM silver_media_spend").fetchone()[0]
    return SilverResult(staged, quarantined, staged - quarantined - silver, silver)


def build_gold(con: duckdb.DuckDBPyConnection, sql_dir: Path) -> dict[str, int]:
    """Run sql/gold/*.sql in filename order; return row counts per model."""
    counts = {}
    for path in sorted((sql_dir / "gold").glob("*.sql")):
        con.execute(path.read_text())
        model = path.stem.split("_", 1)[1]
        counts[model] = con.execute(f"SELECT count(*) FROM {model}").fetchone()[0]
    return counts
