# Databricks notebook source
from datetime import datetime

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.price_obs (
  chain STRING,
  source STRING,
  chain_id STRING,
  sub_chain_id STRING,
  store_id STRING,
  item_code STRING,
  item_type STRING,
  price DECIMAL(10,2),
  unit_price DECIMAL(12,4),
  quantity DECIMAL(12,3),
  unit_qty STRING,
  unit_of_measure STRING,
  qty_in_package DECIMAL(12,3),
  is_weighted BOOLEAN,
  allow_discount BOOLEAN,
  price_updated_at TIMESTAMP,
  file_date DATE,
  file_time STRING,
  source_file STRING,
  collected_date DATE,
  modified_at TIMESTAMP,
  bronze_loaded_at TIMESTAMP
)
CLUSTER BY (file_date, chain)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.quarantine (
  row_key STRING,
  reject_reason STRING,
  chain STRING,
  source STRING,
  chain_id STRING,
  store_id STRING,
  item_code STRING,
  price_raw STRING,
  parse_error STRING,
  file_date DATE,
  source_file STRING,
  collected_date DATE,
  modified_at TIMESTAMP,
  bronze_loaded_at TIMESTAMP,
  header MAP<STRING, STRING>,
  fields MAP<STRING, STRING>,
  quarantined_at TIMESTAMP
)
""")

# COMMAND ----------

last_load = spark.sql("""
SELECT coalesce(max(bronze_loaded_at), TIMESTAMP'1900-01-01') AS t
FROM workspace.silver.price_obs
""").first()["t"]

# An empty quarantine is filled from all of bronze, even if prices are already loaded.
# The price MERGE changes nothing for rows it has already seen.
if spark.table("workspace.silver.quarantine").limit(1).count() == 0:
    last_load = datetime(1900, 1, 1)

print("Reading bronze rows loaded after", last_load)

# COMMAND ----------

classified = spark.sql("""
WITH expected AS (
  SELECT chain, min(chain_id) AS expected_chain_id
  FROM workspace.silver.stores
  GROUP BY chain
),
base AS (
  SELECT
    b.chain,
    b.source,
    trim(coalesce(b.header['ChainID'], b.header['ChainId'])) AS chain_id,
    trim(coalesce(b.header['SubChainID'], b.header['SubChainId'])) AS sub_chain_id,
    regexp_replace(trim(coalesce(b.header['StoreID'], b.header['StoreId'])), '^0+(?=.)', '') AS store_id,
    nullif(trim(b.fields['ItemCode']), '') AS item_code,
    nullif(trim(b.fields['ItemType']), '') AS item_type,
    try_cast(trim(b.fields['ItemPrice']) AS DECIMAL(10,2)) AS price,
    try_cast(trim(b.fields['UnitOfMeasurePrice']) AS DECIMAL(12,4)) AS unit_price,
    try_cast(trim(b.fields['Quantity']) AS DECIMAL(12,3)) AS quantity,
    nullif(trim(b.fields['UnitQty']), '') AS unit_qty,
    nullif(trim(b.fields['UnitOfMeasure']), '') AS unit_of_measure,
    try_cast(trim(b.fields['QtyInPackage']) AS DECIMAL(12,3)) AS qty_in_package,
    trim(b.fields['bIsWeighted']) = '1' AS is_weighted,
    trim(b.fields['AllowDiscount']) = '1' AS allow_discount,
    try_cast(replace(trim(coalesce(b.fields['PriceUpdateDate'], b.fields['PriceUpdateTime'])), 'T', ' ') AS TIMESTAMP) AS price_updated_at,
    to_date(try_to_timestamp(regexp_extract(b.file_name, '-(20[0-9]{6})', 1), 'yyyyMMdd')) AS file_date,
    rpad(regexp_replace(regexp_extract(b.file_name, '-(20[0-9]{6}-?[0-9]{4,6})', 1), '-', ''), 14, '0') AS file_time,
    b.file_path AS source_file,
    b.collected_date,
    b.modified_at,
    b.loaded_at AS bronze_loaded_at,
    b.parse_error,
    b.header,
    b.fields,
    b.fields['ItemPrice'] AS price_raw,
    e.expected_chain_id
  FROM workspace.bronze.price_items b
  LEFT JOIN expected e ON b.chain = e.chain
  WHERE b.loaded_at > :last_load
)
SELECT *,
  CASE
    WHEN parse_error IS NOT NULL THEN 'file_not_readable'
    WHEN NOT (chain_id <=> expected_chain_id) THEN 'wrong_chain'
    WHEN file_date IS NULL THEN 'no_file_date'
    WHEN store_id IS NULL OR store_id = '' THEN 'no_store_id'
    WHEN item_code IS NULL THEN 'no_item_code'
    WHEN price IS NULL THEN 'price_not_a_number'
    WHEN price <= 0 THEN 'price_zero_or_less'
  END AS reject_reason
FROM base
""", args={"last_load": last_load})

classified.createOrReplaceTempView("classified")

# COMMAND ----------

# Rejects first: saving prices moves the watermark forward.
spark.sql("""
MERGE INTO workspace.silver.quarantine AS t
USING (
  SELECT
    sha2(concat_ws('|', source_file, to_json(fields), parse_error), 256) AS row_key,
    reject_reason, chain, source, chain_id, store_id, item_code, price_raw,
    parse_error, file_date, source_file, collected_date, modified_at,
    bronze_loaded_at, header, fields,
    current_timestamp() AS quarantined_at
  FROM classified
  WHERE reject_reason IS NOT NULL
) AS s
ON t.row_key = s.row_key
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

spark.sql("""
MERGE INTO workspace.silver.price_obs AS t
USING (
  SELECT * EXCEPT (parse_error, header, fields, price_raw, expected_chain_id, reject_reason)
  FROM classified
  WHERE reject_reason IS NULL
  QUALIFY row_number() OVER (
    PARTITION BY chain, store_id, item_code, file_date
    ORDER BY CASE source WHEN 'scraper' THEN 1 ELSE 2 END, file_time DESC, modified_at DESC, price_updated_at DESC NULLS LAST, price ASC, source_file
  ) = 1
) AS s
ON  t.file_date = s.file_date
AND t.chain = s.chain
AND t.store_id = s.store_id
AND t.item_code = s.item_code
WHEN MATCHED AND (
     (t.source = 'kaggle' AND s.source = 'scraper')
  OR (t.source = s.source AND s.file_time > t.file_time)
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
