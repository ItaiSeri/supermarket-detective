# Databricks notebook source
TABLE = "workspace.bronze.price_items"
TABLE_PATH = "s3://supermarket-detective-raw-971422709739/bronze/price_items"

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  file_path       STRING,
  file_name       STRING,
  source          STRING,
  chain           STRING,
  collected_date  DATE,
  modified_at     TIMESTAMP,
  declared_count  STRING,
  header          MAP<STRING, STRING>,
  fields          MAP<STRING, STRING>,
  loaded_at       TIMESTAMP
) USING DELTA
LOCATION '{TABLE_PATH}'
""")