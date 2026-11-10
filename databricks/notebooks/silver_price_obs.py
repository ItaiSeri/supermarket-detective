# Databricks notebook source
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

# COMMAND ----------

last_load = spark.sql("""
SELECT coalesce(max(bronze_loaded_at), TIMESTAMP'1900-01-01') AS t
FROM workspace.silver.price_obs
""").first()["t"]

print("Reading bronze rows loaded after", last_load)

# COMMAND ----------

new_obs = spark.sql("""
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
    e.expected_chain_id
  FROM workspace.bronze.price_items b
  LEFT JOIN expected e ON b.chain = e.chain
  WHERE b.loaded_at > :last_load
)
SELECT * EXCEPT (parse_error, expected_chain_id)
FROM base
WHERE parse_error IS NULL
  AND chain_id <=> expected_chain_id
  AND file_date IS NOT NULL
  AND store_id IS NOT NULL AND store_id <> ''
  AND item_code IS NOT NULL
  AND price > 0
QUALIFY row_number() OVER (
  PARTITION BY chain, store_id, item_code, file_date
  ORDER BY CASE source WHEN 'scraper' THEN 1 ELSE 2 END, file_time DESC, modified_at DESC, price_updated_at DESC NULLS LAST, price ASC, source_file
) = 1
""", args={"last_load": last_load})

new_obs.createOrReplaceTempView("new_obs")

# COMMAND ----------

spark.sql("""
MERGE INTO workspace.silver.price_obs AS t
USING new_obs AS s
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
