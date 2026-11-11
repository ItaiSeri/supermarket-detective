# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.items (
  item_key STRING,
  item_scope STRING,
  chain STRING,
  item_code STRING,
  item_type STRING,
  item_name STRING,
  manufacturer_name STRING,
  manufacture_country STRING,
  manufacturer_item_description STRING,
  quantity DECIMAL(12,3),
  unit_qty STRING,
  unit_of_measure STRING,
  is_weighted BOOLEAN,
  chains ARRAY<STRING>,
  chains_count INT,
  names ARRAY<STRING>,
  name_variants INT,
  first_seen DATE,
  last_seen DATE,
  last_file_time STRING,
  last_source_file STRING,
  bronze_loaded_at TIMESTAMP
)
CLUSTER BY (item_key)
""")

# COMMAND ----------

last_load = spark.sql("""
SELECT coalesce(max(bronze_loaded_at), TIMESTAMP'1900-01-01') AS t
FROM workspace.silver.items
""").first()["t"]

print("Reading bronze rows loaded after", last_load)

# COMMAND ----------

# Same reject rules as silver_price_obs.py: an item exists only if one of its rows reached price_obs.
# Key rule: a code is 'shared' (same product in every chain) when ItemType is 1 or the code has the
# shape of a barcode (8, 12 or 13 digits). Everything else is the chain's own code, so the key adds the chain.
incoming_obs = spark.sql("""
WITH expected AS (
  SELECT chain, min(chain_id) AS expected_chain_id
  FROM workspace.silver.stores
  GROUP BY chain
),
base AS (
  SELECT
    b.chain,
    trim(coalesce(b.header['ChainID'], b.header['ChainId'])) AS chain_id,
    regexp_replace(trim(coalesce(b.header['StoreID'], b.header['StoreId'])), '^0+(?=.)', '') AS store_id,
    nullif(trim(b.fields['ItemCode']), '') AS item_code,
    nullif(trim(b.fields['ItemType']), '') AS item_type,
    try_cast(trim(b.fields['ItemPrice']) AS DECIMAL(10,2)) AS price,
    nullif(trim(coalesce(b.fields['ItemName'], b.fields['ItemNm'])), '') AS item_name,
    nullif(trim(coalesce(b.fields['ManufacturerName'], b.fields['ManufactureName'])), '') AS manufacturer_name,
    nullif(trim(coalesce(b.fields['ManufactureCountry'], b.fields['ManufacturerCountry'])), '') AS manufacture_country,
    nullif(trim(coalesce(b.fields['ManufacturerItemDescription'], b.fields['ManufactureItemDescription'])), '') AS manufacturer_item_description,
    try_cast(trim(b.fields['Quantity']) AS DECIMAL(12,3)) AS quantity,
    nullif(trim(b.fields['UnitQty']), '') AS unit_qty,
    nullif(trim(b.fields['UnitOfMeasure']), '') AS unit_of_measure,
    trim(b.fields['bIsWeighted']) = '1' AS is_weighted,
    to_date(try_to_timestamp(regexp_extract(b.file_name, '-(20[0-9]{6})', 1), 'yyyyMMdd')) AS file_date,
    rpad(regexp_replace(regexp_extract(b.file_name, '-(20[0-9]{6}-?[0-9]{4,6})', 1), '-', ''), 14, '0') AS file_time,
    b.file_path AS source_file,
    b.loaded_at AS bronze_loaded_at,
    b.parse_error,
    e.expected_chain_id
  FROM workspace.bronze.price_items b
  LEFT JOIN expected e ON b.chain = e.chain
  WHERE b.loaded_at > :last_load
),
kept AS (
  SELECT * FROM base
  WHERE parse_error IS NULL
    AND chain_id <=> expected_chain_id
    AND file_date IS NOT NULL
    AND store_id IS NOT NULL AND store_id <> ''
    AND item_code IS NOT NULL
    AND price > 0
)
SELECT *,
  CASE WHEN item_type = '1' OR item_code RLIKE '^([0-9]{8}|[0-9]{12,13})$' THEN 'shared' ELSE 'chain' END AS item_scope
FROM kept
""", args={"last_load": last_load})

incoming_obs.createOrReplaceTempView("incoming_obs")

# One row per product from the new rows only. Latest observation = file date, then file time, then load time.
spark.sql("""
CREATE OR REPLACE TEMP VIEW incoming_items AS
SELECT
  CASE WHEN item_scope = 'shared' THEN item_code ELSE concat(chain, ':', item_code) END AS item_key,
  item_scope,
  CASE WHEN item_scope = 'shared' THEN NULL ELSE chain END AS chain,
  item_code,
  latest.item_type, latest.item_name, latest.manufacturer_name, latest.manufacture_country,
  latest.manufacturer_item_description, latest.quantity, latest.unit_qty, latest.unit_of_measure, latest.is_weighted,
  chains, names,
  first_seen, last_seen, latest.file_time AS last_file_time, latest.source_file AS last_source_file,
  max_bronze_loaded_at AS bronze_loaded_at
FROM (
  SELECT
    item_scope,
    CASE WHEN item_scope = 'shared' THEN NULL ELSE chain END AS chain,
    item_code,
    max_by(
      struct(item_type, item_name, manufacturer_name, manufacture_country, manufacturer_item_description,
             quantity, unit_qty, unit_of_measure, is_weighted, file_time, source_file),
      struct(file_date, file_time, bronze_loaded_at, source_file)
    ) AS latest,
    array_sort(collect_set(chain)) AS chains,
    array_sort(collect_set(item_name)) AS names,
    min(file_date) AS first_seen,
    max(file_date) AS last_seen,
    max(bronze_loaded_at) AS max_bronze_loaded_at
  FROM incoming_obs
  GROUP BY item_scope, CASE WHEN item_scope = 'shared' THEN NULL ELSE chain END, item_code
)
""")

# COMMAND ----------

# Combine the new rows with what the table already holds, then MERGE the result.
# Attributes change only when the new observation is not older than the stored one;
# dates, chains and names only widen.
spark.sql("""
MERGE INTO workspace.silver.items AS t
USING (
  SELECT
    i.item_key, i.item_scope, i.chain, i.item_code,
    IF(newer, i.item_type, o.item_type) AS item_type,
    IF(newer, i.item_name, o.item_name) AS item_name,
    IF(newer, i.manufacturer_name, o.manufacturer_name) AS manufacturer_name,
    IF(newer, i.manufacture_country, o.manufacture_country) AS manufacture_country,
    IF(newer, i.manufacturer_item_description, o.manufacturer_item_description) AS manufacturer_item_description,
    IF(newer, i.quantity, o.quantity) AS quantity,
    IF(newer, i.unit_qty, o.unit_qty) AS unit_qty,
    IF(newer, i.unit_of_measure, o.unit_of_measure) AS unit_of_measure,
    IF(newer, i.is_weighted, o.is_weighted) AS is_weighted,
    all_chains AS chains, size(all_chains) AS chains_count,
    all_names AS names, size(all_names) AS name_variants,
    least(i.first_seen, coalesce(o.first_seen, i.first_seen)) AS first_seen,
    IF(newer, i.last_seen, o.last_seen) AS last_seen,
    IF(newer, i.last_file_time, o.last_file_time) AS last_file_time,
    IF(newer, i.last_source_file, o.last_source_file) AS last_source_file,
    greatest(i.bronze_loaded_at, coalesce(o.bronze_loaded_at, i.bronze_loaded_at)) AS bronze_loaded_at
  FROM (
    SELECT i.*,
      o.item_key IS NULL OR struct(i.last_seen, i.last_file_time) >= struct(o.last_seen, o.last_file_time) AS newer,
      array_sort(array_distinct(concat(i.chains, coalesce(o.chains, array())))) AS all_chains,
      array_sort(array_distinct(concat(i.names, coalesce(o.names, array())))) AS all_names
    FROM incoming_items i
    LEFT JOIN workspace.silver.items o ON i.item_key = o.item_key
  ) i
  LEFT JOIN workspace.silver.items o ON i.item_key = o.item_key
) AS s
ON t.item_key = s.item_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
