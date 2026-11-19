# Databricks notebook source
# Gold dimensions (milestone 4, step G1). Marts read only the star (docs/architecture.html, gold layer section).
# This notebook holds the dimensions: dim_date, dim_product and dim_store.
#
#   dim_date     TABLE  one row per day, 2024-09-01 .. 2027-12-31, holidays from a seed file,
#                       plus how many chains have prices on each day (the reason it is rebuilt every run)
#   dim_product  TABLE  one row per product (silver.items) + category columns from silver.product_categories
#   dim_store    TABLE  one row per physical store (silver.stores + silver.store_size_signals), no Shufersal Be,
#                       no Hazi Hinam produce stores

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

# COMMAND ----------

# Holiday seed: databricks/seed/holidays.csv, generated on the laptop by databricks/seed/make_holidays.py
# (a Hebrew-calendar library; the cluster needs none). start_date is the EVE of the holiday, because shoppers
# buy before it; end_date is the last day. The bundle syncs the seed folder next to the notebooks, so the path
# is relative to this notebook (the working directory of a notebook is its own folder).
import pandas as pd

holidays_pd = pd.read_csv("../seed/holidays.csv", parse_dates=["start_date", "end_date"])
assert len(holidays_pd) > 0, "holidays.csv is empty"
spark.createDataFrame(holidays_pd).createOrReplaceTempView("holidays_raw")
# pandas hands over timestamps; the table wants plain dates
spark.sql("""
CREATE OR REPLACE TEMP VIEW holidays_seed AS
SELECT holiday_name, holiday_name_he, CAST(holiday_year AS INT) AS holiday_year,
       CAST(start_date AS DATE) AS start_date, CAST(end_date AS DATE) AS end_date
FROM holidays_raw
""")

# COMMAND ----------

# dim_date. Rules written once here:
#  - weekday: Israeli week, 1 = Sunday ... 7 = Saturday (Spark dayofweek); the weekend is Friday + Saturday;
#    week_start is the Sunday on or before the date.
#  - holiday_name: the holiday whose span (eve .. last day) contains the date, else NULL.
#  - next_holiday_name / days_to_next_holiday: the first holiday whose start (eve) is STRICTLY after the date,
#    so on a holiday day itself (and on its eve) the next one is the following holiday, not the current one.
#  - hanukkah_year / days_from_hanukkah_start: signed days from the nearest Hanukkah start (eve), for the
#    Hanukkah index window (-21 .. +7); NULL when the nearest start is more than 60 days away.
#  - chains_with_prices: distinct chains with rows in silver.price_obs for that file_date (only file_date
#    and chain are read, so the scan stays cheap). data_coverage: full = 8 or more of the 10 chains,
#    partial = 1 to 7, none = 0 (also every future date). This is why the table is rebuilt in every run.
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.dim_date
COMMENT 'One row per day, 2024-09-01 to 2027-12-31. Calendar (Israeli week), Jewish holidays and Hanukkah offsets from the seed file, and price-data coverage per day. Rebuilt in every gold run.'
AS
WITH days AS (
  SELECT explode(sequence(DATE'2024-09-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS date
),
holiday_today AS (
  -- spans never overlap in the seed; min_by keeps the join one row per day regardless
  SELECT d.date, min_by(h.holiday_name, h.start_date) AS holiday_name
  FROM days d JOIN holidays_seed h ON d.date BETWEEN h.start_date AND h.end_date
  GROUP BY d.date
),
holiday_next AS (
  SELECT d.date,
         min_by(h.holiday_name, h.start_date)  AS next_holiday_name,
         min(h.start_date)                     AS next_start
  FROM days d JOIN holidays_seed h ON h.start_date > d.date
  GROUP BY d.date
),
hanukkah_near AS (
  SELECT date, holiday_year AS hanukkah_year, start_date AS hanukkah_start
  FROM (
    SELECT d.date, h.holiday_year, h.start_date,
           row_number() OVER (PARTITION BY d.date ORDER BY abs(datediff(d.date, h.start_date))) AS rn
    FROM days d JOIN holidays_seed h ON h.holiday_name = 'hanukkah'
  )
  WHERE rn = 1 AND abs(datediff(date, start_date)) <= 60
),
coverage AS (
  SELECT file_date AS date, count(DISTINCT chain) AS chains_with_prices
  FROM workspace.silver.price_obs
  GROUP BY file_date
)
SELECT
  d.date,
  year(d.date)                                   AS year,
  month(d.date)                                  AS month,
  date_format(d.date, 'MMMM')                    AS month_name,
  day(d.date)                                    AS day_of_month,
  dayofweek(d.date)                              AS weekday,
  date_format(d.date, 'EEEE')                    AS weekday_name,
  dayofweek(d.date) IN (6, 7)                    AS is_weekend,
  date_sub(d.date, dayofweek(d.date) - 1)        AS week_start,
  ht.holiday_name,
  ht.holiday_name IS NOT NULL                    AS is_holiday,
  coalesce(ht.holiday_name = 'hanukkah', false)  AS is_hanukkah,
  hn.next_holiday_name,
  datediff(hn.next_start, d.date)                AS days_to_next_holiday,
  hk.hanukkah_year,
  datediff(d.date, hk.hanukkah_start)            AS days_from_hanukkah_start,
  coalesce(c.chains_with_prices, 0)              AS chains_with_prices,
  CASE WHEN coalesce(c.chains_with_prices, 0) >= 8 THEN 'full'
       WHEN coalesce(c.chains_with_prices, 0) >= 1 THEN 'partial'
       ELSE 'none' END                           AS data_coverage
FROM days d
LEFT JOIN holiday_today ht ON ht.date = d.date
LEFT JOIN holiday_next  hn ON hn.date = d.date
LEFT JOIN hanukkah_near hk ON hk.date = d.date
LEFT JOIN coverage      c  ON c.date  = d.date
""")

# COMMAND ----------

# Checks: stop the job if the table is not one row per day or the weekday rule is wrong.
n, distinct_n, lo, hi = spark.sql(
    "SELECT count(*), count(DISTINCT date), min(date), max(date) FROM workspace.gold.dim_date").first()
assert n == distinct_n, f"dim_date has repeated dates: {n} rows, {distinct_n} distinct"
assert n == 1217, f"dim_date should have 1217 rows (2024-09-01 .. 2027-12-31), has {n}"
tuesday = spark.sql("SELECT weekday FROM workspace.gold.dim_date WHERE date = DATE'2026-11-17'").first()[0]
assert tuesday == 3, f"2026-11-17 is a Tuesday and should be 3, got {tuesday}"
print(f"dim_date: {n} rows, {lo} .. {hi}")

# COMMAND ----------

# silver.product_categories is the FINAL category table. G1e fills it (rules + ai_classify on active products);
# until then it is created empty so dim_product can already join it and its category columns are NULL.
# The pilot table (silver.product_categories_pilot) is deliberately NOT joined here.
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.product_categories (
  item_key STRING,
  category STRING,
  category_group STRING,
  is_food BOOLEAN,
  confidence DOUBLE,
  source STRING
)
COMMENT 'Final product categories, one row per item_key. Empty until G1e. source = rule, ai_classify or manual.'
""")

# COMMAND ----------

# dim_product: one row per item_key of silver.items (item_key is the product key; the rule lives in
# silver.item_key). chain is NULL for shared products (silver already does that). is_active = seen within
# 14 days of the newest last_seen in the table, so the flag means "still on the shelves", not "ever seen".
# Category columns come from silver.product_categories by LEFT JOIN: NULL until G1e fills it.
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.dim_product
CLUSTER BY (product_key)
COMMENT 'One row per product (silver.items): shared barcode or chain:code. Category columns are NULL until G1e fills silver.product_categories.'
AS
SELECT
  i.item_key                       AS product_key,
  i.item_scope,
  i.item_code,
  i.is_gs1_barcode,
  i.chain,
  i.item_name                      AS product_name,
  i.manufacturer_name              AS manufacturer,
  i.manufacture_country            AS country,
  i.quantity,
  i.unit_qty,
  i.unit_of_measure,
  i.is_weighted,
  i.chains_count,
  i.first_seen,
  i.last_seen,
  i.last_seen >= date_sub(max(i.last_seen) OVER (), 14) AS is_active,
  c.category,
  c.category_group,
  c.is_food,
  c.confidence                     AS category_confidence,
  c.source                         AS category_source
FROM workspace.silver.items i
LEFT JOIN workspace.silver.product_categories c ON c.item_key = i.item_key
""")

# COMMAND ----------

n_items = spark.sql("SELECT count(*) FROM workspace.silver.items").first()[0]
n, distinct_n, active = spark.sql(
    "SELECT count(*), count(DISTINCT product_key), count_if(is_active) FROM workspace.gold.dim_product").first()
assert n == n_items, f"dim_product has {n} rows, silver.items has {n_items}"
assert n == distinct_n, f"dim_product has repeated product_key: {n} rows, {distinct_n} distinct"
print(f"dim_product: {n} rows, {active} active")


# COMMAND ----------

# dim_store: one row per physical store, open or closed (is_open = in a scraper Stores file in the last 14 days). store_key = chain:store_id, the same key the facts build.
# Scope: silver.store_size_signals already holds only physical stores without Shufersal Be (chain SHUFERSAL,
# sub_chain_id 5), so an inner join gives that scope; Hazi Hinam produce stores (store_format_flag = 'produce')
# are removed here. The facts keep only stores found in this table. size_bucket is NULL where the size rules
# had no data. No chain-name column: no chain name mapping exists in silver or the notebooks.
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.dim_store
COMMENT 'One row per physical store, open or closed (Shufersal Be and Hazi Hinam produce stores left out). Key chain:store_id. City, district, Eilat flag and is_open / first_seen / last_seen (from the Stores files) from silver.stores; size from silver.store_size_signals.'
AS
SELECT
  concat(s.chain, ':', s.store_id) AS store_key,
  s.chain,
  s.sub_chain_id,
  s.store_id,
  s.store_name,
  s.address,
  s.city_code_resolved             AS city_code,
  s.city_name_resolved             AS city_name,
  s.city_name_en,
  s.district_name                  AS district,
  coalesce(s.city_code_resolved = 2600, false) AS is_eilat,
  s.store_type,
  s.is_open,
  s.first_seen,
  s.last_seen,
  g.size_bucket,
  g.size_rule,
  g.size_basis
FROM workspace.silver.stores s
JOIN workspace.silver.store_size_signals g ON g.chain = s.chain AND g.store_id = s.store_id
WHERE s.is_physical
  AND NOT (s.chain = 'SHUFERSAL' AND coalesce(s.sub_chain_id, '') = '5')
  AND coalesce(g.store_format_flag, '') <> 'produce'
""")

# COMMAND ----------

n, distinct_n, be, eilat = spark.sql("""
SELECT count(*), count(DISTINCT store_key),
       count_if(chain = 'SHUFERSAL' AND sub_chain_id = '5'), count_if(is_eilat)
FROM workspace.gold.dim_store""").first()
assert n == distinct_n, f"dim_store has repeated store_key: {n} rows, {distinct_n} distinct"
assert be == 0, f"dim_store has {be} Shufersal Be stores"
print(f"dim_store: {n} rows, {eilat} in Eilat")
