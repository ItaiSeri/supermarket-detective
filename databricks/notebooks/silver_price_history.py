# Databricks notebook source
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.price_history
CLUSTER BY (chain, store_id, item_code)
AS
WITH store_days AS (
  SELECT chain, store_id, file_date,
    dense_rank() OVER (PARTITION BY chain, store_id ORDER BY file_date) AS store_day_no,
    max(file_date) OVER (PARTITION BY chain, store_id) AS store_last_date
  FROM (SELECT DISTINCT chain, store_id, file_date FROM workspace.silver.price_obs)
),
obs AS (
  SELECT p.*, d.store_day_no, d.store_last_date,
    struct(p.price, p.unit_price, p.quantity, p.unit_qty, p.unit_of_measure,
           p.qty_in_package, p.is_weighted, p.allow_discount, p.item_type) AS tracked
  FROM workspace.silver.price_obs p
  JOIN store_days d
    ON p.chain = d.chain AND p.store_id = d.store_id AND p.file_date = d.file_date
),
marked AS (
  SELECT *,
    lag(tracked) OVER w AS prev_tracked,
    lag(store_day_no) OVER w AS prev_day_no
  FROM obs
  WINDOW w AS (PARTITION BY chain, store_id, item_code ORDER BY file_date)
),
started AS (
  SELECT *,
    CASE
      WHEN prev_day_no IS NULL THEN 'first_seen'
      WHEN store_day_no - prev_day_no > 1 THEN 'returned'
      WHEN NOT (tracked <=> prev_tracked) THEN 'value_changed'
    END AS start_reason
  FROM marked
),
numbered AS (
  SELECT *,
    count(start_reason) OVER (
      PARTITION BY chain, store_id, item_code
      ORDER BY file_date
      ROWS UNBOUNDED PRECEDING
    ) AS period_no
  FROM started
)
SELECT
  chain,
  store_id,
  item_code,
  period_no,
  any_value(item_type) AS item_type,
  any_value(price) AS price,
  any_value(unit_price) AS unit_price,
  any_value(quantity) AS quantity,
  any_value(unit_qty) AS unit_qty,
  any_value(unit_of_measure) AS unit_of_measure,
  any_value(qty_in_package) AS qty_in_package,
  any_value(is_weighted) AS is_weighted,
  any_value(allow_discount) AS allow_discount,
  min(file_date) AS first_seen,
  max(file_date) AS last_seen,
  count(*) AS days_observed,
  max(start_reason) AS start_reason,
  max(file_date) = any_value(store_last_date) AS is_current,
  min_by(source_file, file_date) AS first_source_file,
  max_by(source_file, file_date) AS last_source_file
FROM numbered
GROUP BY chain, store_id, item_code, period_no
""")
