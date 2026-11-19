# Databricks notebook source
from datetime import datetime

# The SQL functions resolve_dup_keys / has_dup_conflict come from silver_functions (task create_functions)
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

# store_id comes from the file name (see silver_functions.store_id_from_file_name); the header's value is kept next to it.
# store_id_source says where store_id came from: 'file_name', or 'header' when the name had no store part.
# Tables created before 2026-11-17 get the two columns here (old rows stay NULL until reloaded).
_cols = {r["col_name"] for r in spark.sql("DESCRIBE TABLE workspace.silver.price_obs").collect()}
for _c in ("store_id_header", "store_id_source"):
    if _c not in _cols:
        spark.sql(f"ALTER TABLE workspace.silver.price_obs ADD COLUMN {_c} STRING")

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

# Optional one-off re-read after a rule change (empty = normal incremental run):
#   reload_after : timestamp, moves the watermark back
#   reload_chain : read only this chain from bronze (watermark = reload_after, or 1900-01-01 if empty)
# Safe to re-read, because the MERGEs skip rows already saved.
dbutils.widgets.text("reload_after", "")
dbutils.widgets.text("reload_chain", "")
reload_chain = dbutils.widgets.get("reload_chain")
if reload_chain:
    last_load = dbutils.widgets.get("reload_after") or "1900-01-01 00:00:00"
elif dbutils.widgets.get("reload_after"):
    last_load = dbutils.widgets.get("reload_after")

# rebuild_chain = "yes" (needs reload_chain): first delete every row of that chain from price_obs and quarantine,
# then reload it from bronze. Needed when a rule changes the KEY (store_id from the file name, 2026-11-17): the MERGE
# would otherwise leave the old rows with the wrong store next to the new ones.
dbutils.widgets.text("rebuild_chain", "")
rebuild_chain = dbutils.widgets.get("rebuild_chain").lower() == "yes"
if rebuild_chain:
    if not reload_chain:
        raise Exception("rebuild_chain=yes needs reload_chain")
    print("Rebuild: deleting all rows of", reload_chain)
    spark.sql("DELETE FROM workspace.silver.price_obs WHERE chain = :c", args={"c": reload_chain})
    spark.sql("DELETE FROM workspace.silver.quarantine WHERE chain = :c", args={"c": reload_chain})

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
    -- Store from the FILE NAME: Carrefour's in-file StoreID is often an old store code (19 of them are other real stores).
    -- Header value only when the name has no store part.
    coalesce(workspace.silver.store_id_from_file_name(b.file_name),
             regexp_replace(trim(coalesce(b.header['StoreID'], b.header['StoreId'])), '^0+(?=.)', '')) AS store_id,
    regexp_replace(trim(coalesce(b.header['StoreID'], b.header['StoreId'])), '^0+(?=.)', '') AS store_id_header,
    CASE WHEN workspace.silver.store_id_from_file_name(b.file_name) IS NOT NULL THEN 'file_name' ELSE 'header' END AS store_id_source,
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
    try_cast(replace(replace(trim(coalesce(b.fields['PriceUpdateDate'], b.fields['PriceUpdateTime'])), 'T', ' '), '/', '-') AS TIMESTAMP) AS price_updated_at,
    to_date(try_to_timestamp(regexp_extract(b.file_name, '-(20[0-9]{6})', 1), 'yyyyMMdd')) AS file_date,
    rpad(regexp_replace(regexp_extract(b.file_name, '-(20[0-9]{6}-?[0-9]{4,6})', 1), '-', ''), 14, '0') AS file_time,
    b.file_path AS source_file,
    b.collected_date,
    b.modified_at,
    b.loaded_at AS bronze_loaded_at,
    b.parse_error,
    b.header,
    b.fields,
    b.header_orig,
    b.fields_orig,
    b.dup_conflict,
    b.fields['ItemPrice'] AS price_raw,
    e.expected_chain_id
  FROM (
    -- Kaggle files can spell one tag twice (X and X_2 in bronze): resolve once here, keep the originals
    SELECT p.* EXCEPT (header, fields),
           workspace.silver.resolve_dup_keys(p.header) AS header,
           workspace.silver.resolve_dup_keys(p.fields) AS fields,
           p.header AS header_orig,
           p.fields AS fields_orig,
           workspace.silver.has_dup_conflict(p.header) OR workspace.silver.has_dup_conflict(p.fields) AS dup_conflict
    FROM workspace.bronze.price_items p
    WHERE p.loaded_at > :last_load AND (:reload_chain = '' OR p.chain = :reload_chain)
  ) b
  LEFT JOIN expected e ON b.chain = e.chain
  WHERE b.loaded_at > :last_load
),
base_ok AS (
  -- chain_ok: the header ChainID is the chain's own, or a documented alias valid on the file date
  -- (silver.chain_id_aliases, created in silver_functions)
  SELECT base.*,
    (base.chain_id <=> base.expected_chain_id
     OR EXISTS (SELECT 1 FROM workspace.silver.chain_id_aliases a
                WHERE a.chain = base.chain AND a.chain_id = base.chain_id
                  AND base.file_date BETWEEN a.valid_from AND a.valid_to)) AS chain_ok
  FROM base
)
SELECT *,
  CASE
    WHEN parse_error IS NOT NULL THEN 'file_not_readable'
    WHEN dup_conflict THEN 'duplicate_column_conflict'
    WHEN NOT chain_ok THEN 'wrong_chain'
    WHEN file_date IS NULL THEN 'no_file_date'
    WHEN store_id IS NULL OR store_id = '' THEN 'no_store_id'
    WHEN item_code IS NULL THEN 'no_item_code'
    WHEN price IS NULL THEN 'price_not_a_number'
    WHEN price <= 0 THEN 'price_zero_or_less'
    -- provably wrong values (2026-11-19): one Osher Ad row had 72,900,117.25; chains publish 0.01 as a filler
    WHEN price >= 50000 THEN 'absurd_high'
    WHEN price < 0.10 THEN 'placeholder_low'
  END AS reject_reason
FROM base_ok
""", args={"last_load": last_load, "reload_chain": reload_chain})

classified.createOrReplaceTempView("classified")

# COMMAND ----------

# Rejects first: saving prices moves the watermark forward.
spark.sql("""
MERGE INTO workspace.silver.quarantine AS t
USING (
  SELECT
    sha2(concat_ws('|', source_file, to_json(fields_orig), parse_error), 256) AS row_key,
    reject_reason, chain, source, chain_id, store_id, item_code, price_raw,
    parse_error, file_date, source_file, collected_date, modified_at,
    bronze_loaded_at, header_orig AS header, fields_orig AS fields,
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
  SELECT * EXCEPT (parse_error, header, fields, header_orig, fields_orig, dup_conflict, price_raw, expected_chain_id, chain_ok, reject_reason)
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


# COMMAND ----------

# One-off cleanup after the two price rules above were added (2026-11-19): move rows already in price_obs that
# match them (all dates, all sources) to quarantine and delete them from price_obs. Run once with
# apply_price_rules_backfill = yes; running it again moves nothing (the quarantine MERGE skips known rows and
# the DELETE finds nothing). The moved rows have no header / fields (price_obs does not keep them).
dbutils.widgets.text("apply_price_rules_backfill", "")
if dbutils.widgets.get("apply_price_rules_backfill").lower() == "yes":
    rule = "(price >= 50000 OR (price > 0 AND price < 0.10))"
    to_move = spark.sql(f"SELECT count(*) FROM workspace.silver.price_obs WHERE {rule}").first()[0]
    spark.sql(f"""
    MERGE INTO workspace.silver.quarantine AS t
    USING (
      SELECT sha2(concat_ws('|', 'price_rule_backfill', chain, source, store_id, item_code, file_date, source_file), 256) AS row_key,
             CASE WHEN price >= 50000 THEN 'absurd_high' ELSE 'placeholder_low' END AS reject_reason,
             chain, source, chain_id, store_id, item_code, CAST(price AS STRING) AS price_raw,
             CAST(NULL AS STRING) AS parse_error, file_date, source_file, collected_date, modified_at, bronze_loaded_at,
             CAST(NULL AS MAP<STRING, STRING>) AS header, CAST(NULL AS MAP<STRING, STRING>) AS fields,
             current_timestamp() AS quarantined_at
      FROM workspace.silver.price_obs
      WHERE {rule}
    ) AS s
    ON t.row_key = s.row_key
    WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql(f"DELETE FROM workspace.silver.price_obs WHERE {rule}")
    left = spark.sql(f"SELECT count(*) FROM workspace.silver.price_obs WHERE {rule}").first()[0]
    print(f"price rules backfill: {to_move} rows moved from price_obs to quarantine, {left} left in price_obs")
    assert left == 0
