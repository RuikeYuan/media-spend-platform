# Databricks notebook source
# MAGIC %md
# MAGIC # Media spend pipeline on Databricks
# MAGIC
# MAGIC Runs the PySpark pipeline (`src/msp_spark`) against Delta tables in Unity Catalog:
# MAGIC
# MAGIC 1. writes the sample agency deliveries into a Unity Catalog **volume** (the landing zone)
# MAGIC 2. **bronze** – appends only files not seen before, each row as JSON with file lineage
# MAGIC 3. **silver** – data contracts, FX to EUR, quarantine, latest delivery wins
# MAGIC 4. **gold** – star schema + monthly advertiser mart
# MAGIC 5. **data quality** – the same YAML checks as the local pipeline; error-level failures fail the job
# MAGIC
# MAGIC Works on serverless compute, including Databricks Free Edition. Open it from a Git folder
# MAGIC cloned from `https://github.com/RuikeYuan/media-spend-platform`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "media_spend")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

import os
import sys
from pathlib import Path

# The notebook lives in <repo>/databricks/, so the repo root is one level up.
REPO = Path(os.getcwd()).parent
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))
# After a Git pull, make sure this session imports the new code, not a cached module.
for name in [m for m in sys.modules if m == "msp" or m.startswith(("msp.", "msp_spark"))]:
    del sys.modules[name]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.files")

os.environ["MSP_CONFIG_DIR"] = str(REPO / "config")
os.environ["MSP_SQL_DIR"] = str(REPO / "sql")
os.environ["MSP_DATA_DIR"] = f"/Volumes/{CATALOG}/{SCHEMA}/files"
print("repo:", REPO, "| landing:", os.environ["MSP_DATA_DIR"] + "/landing")

# COMMAND ----------

# MAGIC %md ## Land the sample deliveries (idempotent: identical files, so reruns load nothing new)

# COMMAND ----------

from msp.generate import generate
from msp.settings import Settings

settings = Settings()
written = generate(settings.landing_dir, late=True)
print(f"{len(written)} files in {settings.landing_dir}")

# COMMAND ----------

# MAGIC %md ## Run bronze → silver → gold → quality checks

# COMMAND ----------

from msp_spark.pipeline import SparkEnv, run

env = SparkEnv.create(catalog=CATALOG, schema=SCHEMA)
outcome = run(settings, env)

# COMMAND ----------

# MAGIC %md ## Results

# COMMAND ----------

display(spark.sql("SELECT check_name, severity, status, details FROM dq_results "
                  f"WHERE run_id = '{outcome.run_id}' ORDER BY status DESC, check_name"))

# COMMAND ----------

display(spark.sql("SELECT reject_reason, source, count(*) AS rows FROM quarantine_media_rows GROUP BY ALL"))

# COMMAND ----------

display(spark.table("mart_advertiser_monthly"))

# COMMAND ----------

# Delta keeps every version of the table: this is the audit trail behind each number.
display(spark.sql("DESCRIBE HISTORY silver_media_spend").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# Fail the job run (and alert) when an error-level data quality check failed.
if outcome.exit_code != 0:
    raise RuntimeError(f"Pipeline {outcome.status}: see dq_results for run {outcome.run_id}")
