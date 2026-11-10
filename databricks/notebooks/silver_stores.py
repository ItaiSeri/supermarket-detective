# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.stores AS
WITH parsed AS (
  SELECT
    chain,
    coalesce(header['ChainID'], header['ChainId']) AS chain_id,
    coalesce(header['SubChainID'], header['SubChainId']) AS sub_chain_id,
    fields['StoreID'] AS store_id_raw,
    regexp_replace(fields['StoreID'], '^0+(?=.)', '') AS store_id,
    fields['StoreName'] AS store_name,
    fields['Address'] AS address,
    nullif(trim(fields['City']), '') AS city_raw,
    nullif(regexp_replace(coalesce(fields['ZIPCode'], fields['ZipCode']), '^0+$', ''), '') AS zip_code,
    fields['StoreType'] AS store_type,
    to_date(regexp_extract(file_name, '-(20[0-9]{6})', 1), 'yyyyMMdd') AS file_date,
    modified_at,
    file_path
  FROM workspace.bronze.stores
  WHERE parse_error IS NULL
),
latest AS (
  SELECT *,
    row_number() OVER (PARTITION BY chain, store_id ORDER BY file_date DESC, modified_at DESC) AS rn
  FROM parsed
),
flagged AS (
  SELECT *, coalesce(store_type = '1', false) AS is_physical
  FROM latest
  WHERE rn = 1
)
SELECT
  chain,
  chain_id,
  sub_chain_id,
  store_id,
  store_id_raw,
  store_name,
  address,
  CASE WHEN city_raw RLIKE '^[1-9][0-9]*$' THEN CAST(city_raw AS INT) END AS city_code,
  CASE WHEN city_raw RLIKE '[\u0590-\u05FF]' THEN city_raw END AS city_name,
  zip_code,
  store_type,
  is_physical,
  is_physical AND coalesce(city_raw IN ('2600', '\u05D0\u05D9\u05DC\u05EA') OR zip_code LIKE '88%', false) AS is_eilat,
  file_date AS last_file_date,
  file_path AS source_file
FROM flagged
""")