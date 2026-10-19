"""Bronze -> silver -> gold on Spark.

Runs unchanged locally (plain Spark, Parquet tables) and on Databricks (Delta
tables in Unity Catalog). Differences between the two are confined to `SparkEnv`.

    python -m msp_spark.pipeline              # local: generates sample data if missing, then runs

Layers
  bronze_raw            every delivered row as a JSON payload + file lineage (append-only)
  silver_media_spend    contracts applied, FX to EUR, quarantined rows removed, latest delivery wins
  quarantine_media_rows rows that broke a hard rule, with the reason
  gold                  dim_date, dim_campaign, dim_channel, fact_media_spend, mart_advertiser_monthly
"""
from __future__ import annotations

import csv
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from msp import quality
from msp.settings import Settings

NATURAL_KEY = ["source", "activity_date", "campaign_id", "channel", "publisher"]

# Python strftime (used by the contracts) -> Spark datetime pattern
_FMT = {"%Y": "yyyy", "%m": "MM", "%d": "dd"}


def spark_date_format(py_fmt: str) -> str:
    out = py_fmt
    for py, sp in _FMT.items():
        out = out.replace(py, sp)
    return out


# ------------------------------------------------------------------ environment --
@dataclass
class SparkEnv:
    """Where tables live and in which format. Detected automatically."""

    spark: SparkSession
    on_databricks: bool
    table_format: str          # "delta" on Databricks, "parquet" locally
    catalog: str | None = None
    schema: str = "media_spend"

    @classmethod
    def create(cls, catalog: str | None = None, schema: str = "media_spend") -> SparkEnv:
        on_dbx = "DATABRICKS_RUNTIME_VERSION" in os.environ
        if on_dbx:
            spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        else:
            spark = (SparkSession.builder.master("local[2]").appName("media-spend-platform")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "4")
                     .config("spark.sql.session.timeZone", "UTC")
                     .getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
        env = cls(spark, on_dbx, "delta" if on_dbx else "parquet", catalog, schema)
        if catalog:
            spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        spark.sql(f"USE {schema}")
        return env

    def table_exists(self, name: str) -> bool:
        return self.spark.catalog.tableExists(name)

    def write(self, df: DataFrame, name: str, mode: str = "overwrite") -> int:
        (df.write.format(self.table_format).mode(mode)
           .option("overwriteSchema", "true").saveAsTable(name))
        return self.spark.table(name).count()


class SparkCon:
    """Minimal DB-API-like adapter so msp.quality runs its checks on Spark."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def execute(self, sql: str):
        rows = self.spark.sql(sql).collect()

        class _Result:
            def fetchone(self):
                return tuple(rows[0]) if rows else None

            def fetchall(self):
                return [tuple(r) for r in rows]

        return _Result()


# ------------------------------------------------------------------------ seeds --
def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def load_seeds(env: SparkEnv, settings: Settings, contracts: dict) -> None:
    """Small reference data is read in Python, so it also works where Spark can't read workspace files."""
    spark = env.spark
    seeds = settings.config_dir / "seeds"
    spark.createDataFrame(_read_csv(seeds / "campaigns.csv")).createOrReplaceTempView("seed_campaigns")
    fx = [(r["currency"], datetime.strptime(r["month"], "%Y-%m-%d").date(), float(r["rate_to_eur"]))
          for r in _read_csv(seeds / "fx_rates.csv")]
    spark.createDataFrame(fx, "currency string, month date, rate_to_eur double").createOrReplaceTempView("seed_fx_rates")
    channel_map = [(s, raw.lower(), canonical)
                   for s, c in contracts.items() for raw, canonical in c["channel_map"].items()]
    spark.createDataFrame(channel_map, "source string, channel_raw string, channel string") \
         .createOrReplaceTempView("map_channel")


# ----------------------------------------------------------------------- bronze --
@dataclass
class BronzeResult:
    new_files: list[str] = field(default_factory=list)
    skipped_files: int = 0
    rejected_files: list[tuple[str, str]] = field(default_factory=list)
    rows_loaded: int = 0


def _read_source(spark: SparkSession, folder: Path, contract: dict) -> DataFrame | None:
    files = sorted(folder.glob(contract["file_pattern"]))
    if not files:
        return None
    paths = [str(p) for p in files]
    if contract["format"] == "csv":
        return (spark.read.option("header", True).option("sep", contract.get("delimiter", ","))
                .option("inferSchema", False).option("encoding", "UTF-8").csv(paths))
    if contract["format"] == "jsonl":
        return spark.read.option("primitivesAsString", True).json(paths)
    raise ValueError(f"Unsupported format {contract['format']!r}")


def ingest_bronze(env: SparkEnv, contracts: dict, landing_dir: Path, run_id: str) -> BronzeResult:
    """Append rows from files not loaded before. Idempotent by file name + modification time.

    On Databricks the same job can be done by Auto Loader (`cloudFiles`), which keeps
    this bookkeeping in a checkpoint; batch mode is used here so it also runs locally.
    """
    spark = env.spark
    result = BronzeResult()
    already = set()
    if env.table_exists("ingestion_log"):
        already = {(r.file_name, r.file_mtime) for r in
                   spark.table("ingestion_log").select("file_name", "file_mtime").collect()}

    for source, contract in contracts.items():
        df = _read_source(spark, landing_dir / source, contract)
        if df is None:
            continue
        cols = [c for c in df.columns if not c.startswith("_corrupt")]
        staged = (df.select(
                    F.lit(source).alias("source"),
                    F.col("_metadata.file_name").alias("file_name"),
                    F.col("_metadata.file_modification_time").alias("file_mtime"),
                    F.to_json(F.struct(*[F.col(f"`{c}`") for c in cols])).alias("payload"))
                  .withColumn("row_id", F.monotonically_increasing_id()))

        # Schema drift: a file missing a contract column is rejected as a whole.
        required = set(contract["columns"].values())
        missing = sorted(required - set(cols))
        files = staged.select("file_name", "file_mtime").distinct().collect()
        new = [(f.file_name, f.file_mtime) for f in files if (f.file_name, f.file_mtime) not in already]
        result.skipped_files += len(files) - len(new)
        if not new:
            continue
        if missing:
            # (with mixed layouts in one folder Spark unions the headers; a per-file check would go here)
            for name, _ in new:
                result.rejected_files.append((name, f"missing required columns: {missing}"))
            continue

        new_df = spark.createDataFrame(new, "file_name string, file_mtime timestamp")
        batch = (staged.join(F.broadcast(new_df), ["file_name", "file_mtime"])
                       .withColumn("run_id", F.lit(run_id))
                       .withColumn("ingested_at", F.current_timestamp()))
        n = batch.count()
        env.write(batch, "bronze_raw", mode="append")
        log = (batch.groupBy("source", "file_name", "file_mtime", "run_id")
                    .agg(F.count("*").alias("row_count"), F.max("ingested_at").alias("ingested_at")))
        env.write(log, "ingestion_log", mode="append")
        result.new_files += [name for name, _ in new]
        result.rows_loaded += n
    return result


# ----------------------------------------------------------------------- silver --
def _field(name: str) -> Column:
    key = name.replace("'", "\\'")
    return F.get_json_object(F.col("payload"), f"$['{key}']")


def staged(env: SparkEnv, source: str, contract: dict) -> DataFrame:
    cols = contract["columns"]
    dec = contract.get("decimal_separator", ".")
    fmt = spark_date_format(contract["date_format"])
    currency = (F.upper(F.trim(_field(cols["currency"]))) if "currency" in cols
                else F.lit(contract["currency"]))
    df = env.spark.table("bronze_raw").where(F.col("source") == source)
    df = df.select(
        "source", "file_name", "ingested_at", "row_id", "payload",
        F.trim(_field(cols["activity_date"])).alias("_date_raw"),
        F.nullif(F.trim(_field(cols["campaign_id"])), F.lit("")).alias("campaign_id"),
        F.lower(F.trim(_field(cols["channel"]))).alias("channel_raw"),
        F.trim(_field(cols["publisher"])).alias("publisher"),
        _clean_number(_field(cols["impressions"]), dec).alias("_imps_raw"),
        _clean_number(_field(cols["clicks"]), dec).alias("_clicks_raw"),
        _clean_number(_field(cols["spend"]), dec).alias("_spend_raw"),
        currency.alias("currency"))
    df.createOrReplaceTempView("_stg")
    # try_* functions return NULL for unparseable input instead of failing the job,
    # which is what lets bad rows be quarantined rather than crash the pipeline.
    return env.spark.sql(f"""
        SELECT source, file_name, ingested_at, row_id, payload,
               CAST(try_to_timestamp(_date_raw, '{fmt}') AS DATE) AS activity_date,
               campaign_id, channel_raw, publisher,
               try_cast(_imps_raw AS BIGINT)        AS impressions,
               try_cast(_clicks_raw AS BIGINT)      AS clicks,
               try_cast(_spend_raw AS DECIMAL(18,2)) AS spend_local,
               currency
        FROM _stg""")


def _clean_number(col: Column, decimal_separator: str) -> Column:
    col = F.trim(col)
    if decimal_separator == ",":
        return F.regexp_replace(F.regexp_replace(col, r"\.", ""), ",", ".")
    return F.regexp_replace(col, ",", "")


@dataclass
class SilverResult:
    staged_rows: int
    quarantined_rows: int
    superseded_rows: int
    silver_rows: int


def build_silver(env: SparkEnv, contracts: dict) -> SilverResult:
    spark = env.spark
    stg = None
    for source, contract in contracts.items():
        part = staged(env, source, contract)
        stg = part if stg is None else stg.unionByName(part)

    m = spark.table("map_channel")
    fx = spark.table("seed_fx_rates")
    v = (stg.join(m, ["source", "channel_raw"], "left")
            .join(fx, (fx.currency == stg.currency)
                      & (fx.month == F.trunc(stg.activity_date, "month")), "left")
            .drop(fx.currency).drop("month"))
    v = v.withColumn("spend_eur", F.round(F.col("spend_local") * F.col("rate_to_eur"), 2).cast("decimal(18,2)"))

    reasons = F.concat_ws(", ",
        F.when(F.col("activity_date").isNull(), F.lit("invalid_date")),
        F.when(F.col("campaign_id").isNull(), F.lit("missing_campaign_id")),
        F.when(F.col("impressions").isNull() | F.col("clicks").isNull() | F.col("spend_local").isNull(),
               F.lit("non_numeric_metric")),
        F.when((F.col("impressions") < 0) | (F.col("clicks") < 0) | (F.col("spend_local") < 0),
               F.lit("negative_metric")),
        F.when(F.col("channel").isNull(),
               F.concat(F.lit("unmapped_channel:"), F.coalesce(F.col("channel_raw"), F.lit("null")))),
        F.when(F.col("activity_date").isNotNull() & F.col("rate_to_eur").isNull(),
               F.concat(F.lit("missing_fx_rate:"), F.coalesce(F.col("currency"), F.lit("null")))))
    v = v.withColumn("reject_reason", F.nullif(reasons, F.lit(""))).cache()

    quarantine = (v.where(F.col("reject_reason").isNotNull())
                   .select("source", "file_name", "row_id", "reject_reason", "payload"))
    q = env.write(quarantine, "quarantine_media_rows")

    # Latest delivery wins: a restatement (later file) replaces earlier rows for the same
    # natural key; exact duplicates inside a file collapse to one row.
    w = Window.partitionBy(*NATURAL_KEY).orderBy(
        F.col("ingested_at").desc(), F.col("file_name").desc(), F.col("row_id").desc())
    silver = (v.where(F.col("reject_reason").isNull())
               .withColumn("rn", F.row_number().over(w)).where("rn = 1")
               .select(*NATURAL_KEY, "impressions", "clicks", "spend_local", "currency", "spend_eur",
                       F.col("file_name").alias("loaded_file_name"), F.col("row_id").alias("loaded_row_id")))
    s = env.write(silver, "silver_media_spend")
    staged_rows = v.count()
    v.unpersist()
    return SilverResult(staged_rows, q, staged_rows - q - s, s)


MERGE_SILVER_SQL = """
MERGE INTO silver_media_spend AS t
USING silver_updates AS s
  ON  t.source = s.source AND t.activity_date = s.activity_date AND t.campaign_id = s.campaign_id
  AND t.channel = s.channel AND t.publisher = s.publisher
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""
"""Incremental alternative to the full rebuild, for Delta tables at scale: stage only the
rows from newly ingested files as `silver_updates`, then MERGE on the natural key. Because
new files are later deliveries, MATCHED rows are exactly the restatements."""


# ------------------------------------------------------------------------- gold --
GOLD_MODELS = ["dim_date", "dim_campaign", "dim_channel", "fact_media_spend", "mart_advertiser_monthly"]


def build_gold(env: SparkEnv, sql_dir: Path) -> dict[str, int]:
    counts = {}
    for path in sorted((sql_dir / "gold_spark").glob("*.sql")):
        model = path.stem.split("_", 1)[1]
        counts[model] = env.write(env.spark.sql(path.read_text()), model)
    return counts


# ---------------------------------------------------------------------- runner --
@dataclass
class SparkOutcome:
    run_id: str
    status: str
    checks: list
    metrics: dict

    @property
    def exit_code(self) -> int:
        return {"success": 0, "success_with_warnings": 0, "failed": 1}.get(self.status, 2)


def run(settings: Settings | None = None, env: SparkEnv | None = None, log=print) -> SparkOutcome:
    settings = settings or Settings()
    env = env or SparkEnv.create()
    contracts = settings.contracts()
    run_id = uuid.uuid4().hex[:8]
    metrics: dict = {}
    log(f"Run {run_id} on {'Databricks' if env.on_databricks else 'local Spark'} ({env.table_format} tables)")

    load_seeds(env, settings, contracts)
    b = ingest_bronze(env, contracts, settings.landing_dir, run_id)
    metrics["bronze"] = dict(new_files=len(b.new_files), skipped_files=b.skipped_files,
                             rejected_files=len(b.rejected_files), rows_loaded=b.rows_loaded)
    log(f"  bronze  {metrics['bronze']}")
    for name, problem in b.rejected_files:
        log(f"  ALERT file rejected: {name} — {problem}")
    s = build_silver(env, contracts)
    metrics["silver"] = s.__dict__
    log(f"  silver  {metrics['silver']}")
    metrics["gold"] = build_gold(env, settings.sql_dir)
    log(f"  gold    {metrics['gold']}")

    checks = quality.evaluate(SparkCon(env.spark), settings.quality_checks(), engine="spark")
    dq = env.spark.createDataFrame(
        [(run_id, c.name, c.type, c.severity, c.status, c.observed, c.details) for c in checks],
        "run_id string, check_name string, check_type string, severity string, status string, "
        "observed double, details string").withColumn("checked_at", F.current_timestamp())
    env.write(dq, "dq_results", mode="append")

    failures = [c for c in checks if c.status == "fail"]
    warnings = [c for c in checks if c.status == "warn"]
    for c in failures + warnings:
        log(f"  {'FAIL' if c.status == 'fail' else 'WARN'} {c.name}: {c.details}")
    status = "failed" if failures else ("success_with_warnings" if warnings or b.rejected_files else "success")
    log(f"Run {run_id}: {status.upper()} — {len(checks) - len(failures) - len(warnings)} passed, "
        f"{len(warnings)} warned, {len(failures)} failed")
    return SparkOutcome(run_id, status, checks, metrics)


def main() -> int:
    from msp.generate import generate

    settings = Settings()
    if not any(settings.landing_dir.glob("*/*")):
        generate(settings.landing_dir, late=True)
    return run(settings).exit_code


if __name__ == "__main__":
    sys.exit(main())
