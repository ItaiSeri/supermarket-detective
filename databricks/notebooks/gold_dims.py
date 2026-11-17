# Databricks notebook source
# Gold dimensions (milestone 4, step G1). Marts read only the star (docs/architecture.html, gold layer section).
# This notebook holds the dimensions: dim_date now; dim_store and dim_product are added in later steps.
#
#   dim_date  TABLE  one row per day, 2024-09-01 .. 2027-12-31, holidays from a seed file,
#                    plus how many chains have prices on each day (the reason it is rebuilt every run)

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
