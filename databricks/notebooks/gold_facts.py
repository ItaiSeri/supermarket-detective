# Databricks notebook source
# Gold facts (milestone 4, step G0). The star's facts; marts read only the star (docs/architecture.html, gold layer section).
# Rule: a fact is a stored table only when building it does real work (changes the grain);
# otherwise it is a view over silver (no second copy).
#   fact_price_period     VIEW   silver.price_history already has the grain; keys + renames only
#   fact_promotion_period TABLE  silver.promotion_obs (one row per day) collapsed to one row per deal
# Store-wide promotions (coupons, donations, delivery fees; silver.store_wide_promotions) are not in gold:
# decided by Itai 2026-11-17, they are not deals on products.
# Both facts keep only stores that are in gold.dim_store (physical, no Shufersal Be, no produce stores), so facts
# and dimension agree; gold_load builds dim_store first. fact_price_period also leaves out a price period when
# silver.price_flags has an 'exclude' row (absurd or placeholder price) for the same store and product, the same
# price and a date inside the period. Flags cover only the last 14 days, so older bad prices are not caught yet.

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
spark.sql("DROP VIEW IF EXISTS workspace.gold.fact_store_wide_promo")

# COMMAND ----------

# Shelf prices: one row per store x product x price period (SCD2 periods from silver).
spark.sql("""
CREATE OR REPLACE VIEW workspace.gold.fact_price_period
COMMENT 'One row per store x product x price period. View over silver.price_history with gold keys.'
AS
SELECT
  d.store_key                                           AS store_key,
  workspace.silver.item_key(h.chain, h.item_code, h.item_type) AS product_key,
  h.price,
  h.unit_price,
  h.quantity,
  h.unit_of_measure,
  h.is_weighted,
  h.first_seen AS valid_from,
  h.last_seen  AS valid_to,
  h.days_observed,
  h.is_current
FROM workspace.silver.price_history h
JOIN workspace.gold.dim_store d ON d.chain = h.chain AND d.store_id = h.store_id
LEFT ANTI JOIN (SELECT * FROM workspace.silver.price_flags WHERE action = 'exclude') f
  ON f.chain = h.chain AND f.store_id = h.store_id AND f.item_code = h.item_code AND f.price = h.price
 AND f.file_date BETWEEN h.first_seen AND h.last_seen
""")

# COMMAND ----------

# Product deals: one row per store x promotion x group x product (decided by Itai 2026-11-17).
# A promotion ID is the deal's identity, so a period is never split by its values: chains re-encode the same
# deal from day to day (Shufersal's 1+1 written as 50% / 19.90 one day and 100% / 0.00 the next), and splitting
# would count one deal several times. Deal values come from the last day seen; values_changed flags a deal whose
# values differed between days (the day-by-day detail stays in silver.promotion_obs).
# A product can sit in several promotions at once (e.g. "2 for 20" and a coupon): each is its own row.
# Full rebuild for now; incremental MERGE (extend or insert) after the backfill era.
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.fact_promotion_period
CLUSTER BY (store_key, product_key)
COMMENT 'One row per store x promotion x group x product. Built from silver.promotion_obs.'
AS
WITH obs AS (
  SELECT o.*,
    xxhash64(description, reward_type, min_qty, max_qty, discounted_price, discounted_price_per_unit,
             discount_rate, discount_type, club_code, is_coupon, is_gift_item, allow_multiple_discounts,
             min_purchase_amount, start_at, end_at) AS deal_hash
  FROM workspace.silver.promotion_obs o
  JOIN workspace.gold.dim_store d ON d.chain = o.chain AND d.store_id = o.store_id
),
deals AS (
  SELECT
    chain, store_id, promotion_id, group_id, item_code,
    max_by(struct(item_type, description, reward_type, min_qty, max_qty, discounted_price,
                  discounted_price_per_unit, discount_rate, discount_type, club_code, is_coupon, is_gift_item,
                  allow_multiple_discounts, min_purchase_amount, start_at, end_at, source), file_date) AS last,
    min(file_date)            AS first_seen,
    max(file_date)            AS last_seen,
    count(*)                  AS days_observed,
    count(DISTINCT deal_hash) > 1 AS values_changed
  FROM obs
  GROUP BY chain, store_id, promotion_id, group_id, item_code
)
SELECT
  concat(chain, ':', store_id)                               AS store_key,
  workspace.silver.item_key(chain, item_code, last.item_type) AS product_key,
  promotion_id,
  group_id,
  last.description,
  last.reward_type,
  last.min_qty,
  last.max_qty,
  last.discounted_price          AS deal_price,
  last.discounted_price_per_unit AS deal_unit_price,
  last.discount_rate,
  last.discount_type,
  last.club_code,
  last.is_coupon,
  last.is_gift_item,
  last.allow_multiple_discounts,
  last.min_purchase_amount,
  last.start_at,
  last.end_at,
  first_seen,
  last_seen,
  days_observed,
  values_changed,
  last.source
FROM deals
""")

# COMMAND ----------

# Reconciliation: every daily row of silver (for stores in dim_store) is in exactly one deal row, and the price
# view loses only what it is meant to drop: stores outside dim_store and periods with an 'exclude' flag.
check = spark.sql("""
SELECT
  (SELECT sum(days_observed) FROM workspace.gold.fact_promotion_period) AS promo_days_gold,
  (SELECT count(*) FROM workspace.silver.promotion_obs o
     JOIN workspace.gold.dim_store d ON d.chain = o.chain AND d.store_id = o.store_id) AS promo_rows_silver,
  (SELECT count(*)           FROM workspace.gold.fact_promotion_period) AS promo_deals,
  (SELECT count(*)           FROM workspace.gold.fact_price_period)     AS price_periods_gold,
  (SELECT count(*) FROM workspace.silver.price_history h
     JOIN workspace.gold.dim_store d ON d.chain = h.chain AND d.store_id = h.store_id) AS price_periods_in_scope
""").first()
print(check)
assert check.promo_days_gold == check.promo_rows_silver, "fact_promotion_period does not add up to promotion_obs"
assert check.price_periods_gold <= check.price_periods_in_scope, "fact_price_period has more rows than silver.price_history"
