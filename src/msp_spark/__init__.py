"""PySpark / Databricks implementation of the media spend pipeline.

Same data contracts (config/sources.yml), same quality checks
(config/quality_checks.yml) and same medallion layers as the DuckDB version in
`msp`, written for Spark so it runs on a laptop or on Databricks with Delta Lake.
"""
