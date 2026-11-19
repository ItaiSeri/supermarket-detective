# Databricks notebook source
# Price flags (cleansing: "flag, don't delete"). One row per suspicious scraper observation of the last 14 file
# dates; price_obs itself is never changed. Rebuilt in full each run.
#   absurd_high        price >= 50,000                                  action exclude
#   placeholder_low    price < 0.10 (chains publish 0.01 as a filler)   action exclude
#   chain_outlier_high price >= 10x the chain's median for that item    action observe (needs >= 5 stores)
#   chain_outlier_low  price <= 0.1x the chain's median for that item   action observe (needs >= 5 stores)
# The median is taken within the same chain only: internal produce/deli codes (7290000000xxx) are reused by
# different chains for different products, so a median across chains would flag good prices.
# 'exclude' rows are left out of gold's price facts; 'observe' rows are only listed (observe mode first).
# If several rules hit one row, the first in the order above is kept.

spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.price_flags
COMMENT 'Suspicious scraper prices of the last 14 file dates, one row per flagged observation. action exclude = left out of gold price facts; observe = listed only. price_obs is not changed. Rebuilt every run.'
AS
WITH recent AS (
  SELECT chain, store_id, item_code, file_date, price
  FROM workspace.silver.price_obs
  WHERE source = 'scraper'
    AND file_date >= date_sub((SELECT max(file_date) FROM workspace.silver.price_obs WHERE source = 'scraper'), 13)
),
stats AS (
  SELECT chain, item_code, file_date,
         percentile(price, 0.5) AS chain_median,
         count(DISTINCT store_id) AS stores_in_chain
  FROM recent
  GROUP BY chain, item_code, file_date
),
scored AS (
  SELECT r.chain, r.store_id, r.item_code, r.file_date, r.price,
         s.chain_median, s.stores_in_chain,
         r.price / nullif(s.chain_median, 0) AS ratio
  FROM recent r
  JOIN stats s ON s.chain = r.chain AND s.item_code = r.item_code AND s.file_date = r.file_date
),
flagged AS (
  SELECT *,
    CASE WHEN price >= 50000 THEN 'absurd_high'
         WHEN price < 0.10 THEN 'placeholder_low'
         WHEN ratio >= 10 AND stores_in_chain >= 5 THEN 'chain_outlier_high'
         WHEN ratio <= 0.1 AND stores_in_chain >= 5 THEN 'chain_outlier_low'
    END AS flag_reason
  FROM scored
)
SELECT chain, store_id, item_code, file_date, price, chain_median, stores_in_chain, ratio, flag_reason,
       CASE WHEN flag_reason IN ('absurd_high', 'placeholder_low') THEN 'exclude' ELSE 'observe' END AS action
FROM flagged
WHERE flag_reason IS NOT NULL
""")

# COMMAND ----------

n, distinct_n = spark.sql(
    "SELECT count(*), count(DISTINCT chain, store_id, item_code, file_date) FROM workspace.silver.price_flags").first()
assert n == distinct_n, f"price_flags has repeated keys: {n} rows, {distinct_n} distinct"
print(f"price_flags: {n} rows")
spark.sql("SELECT flag_reason, action, count(*) AS n FROM workspace.silver.price_flags GROUP BY ALL ORDER BY n DESC").show()
