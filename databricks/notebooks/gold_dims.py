# Databricks notebook source
# Gold dimensions (milestone 4, step G1). Marts read only the star (docs/architecture.html, gold layer section).
# This notebook holds the dimensions: dim_date and dim_product now; dim_store is added in a later step.
#
#   dim_date     TABLE  one row per day, 2024-09-01 .. 2027-12-31, holidays from a seed file,
#                       plus how many chains have prices on each day (the reason it is rebuilt every run)
#   dim_product  TABLE  one row per product (silver.items) + category columns from silver.product_categories

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

# store_size_signals (G1b-2a): two cheap signals of how big each store is, saved for review. A helper table for
# dim_store, not for Power BI. Nothing here decides a store's final size.
# Scope: physical stores except Shufersal Be (sub_chain_id 5, out of gold). One row per chain + store_id.
#  Signal 1: median over the last 30 days of the number of price rows per file_date (needs 5+ days of files),
#            as a ratio to the chain's 75th percentile; per-chain gap rule below.
#  Signal 2: location keywords (seed/store_location_keywords.csv) found as whole words in store_name or address.
#  format_label / label_size: the chain's own format, for comparison only.
import re
import numpy as np

kw_pd = pd.read_csv("../seed/store_location_keywords.csv")
assert len(kw_pd) > 0 and set(kw_pd.columns) == {"keyword", "location_type"}, "bad keyword seed"

base_pd = spark.sql("""
WITH scope AS (
  SELECT chain, store_id, store_name, address, source_file
  FROM workspace.silver.stores
  WHERE is_physical AND NOT (chain = 'SHUFERSAL' AND sub_chain_id = '5')
),
daily AS (
  SELECT p.chain, p.store_id, p.file_date, count(*) AS n
  FROM workspace.silver.price_obs p
  JOIN scope s ON s.chain = p.chain AND s.store_id = p.store_id
  WHERE p.file_date >= date_sub(current_date(), 30)
  GROUP BY p.chain, p.store_id, p.file_date
),
sig AS (
  SELECT chain, store_id, count(*) AS days_observed, percentile(n, 0.5) AS median_products
  FROM daily GROUP BY chain, store_id
),
labels AS (
  SELECT s.chain, s.store_id,
         max(coalesce(b.fields['SubChainName'], b.header['SubChainName'])) AS sub_chain_name
  FROM scope s
  JOIN workspace.bronze.stores b
    ON b.file_path = s.source_file
   AND regexp_replace(b.fields['StoreID'], '^0+(?=.)', '') = s.store_id
  WHERE s.chain = 'SHUFERSAL' OR s.chain LIKE 'MAHSANI%'
  GROUP BY s.chain, s.store_id
)
SELECT s.chain, s.store_id, s.store_name, s.address,
       coalesce(g.days_observed, 0) AS days_observed, g.median_products, l.sub_chain_name
FROM scope s
LEFT JOIN sig g ON g.chain = s.chain AND g.store_id = s.store_id
LEFT JOIN labels l ON l.chain = s.chain AND l.store_id = s.store_id
""").toPandas()
n_scope = spark.sql("""SELECT count(*) FROM workspace.silver.stores
  WHERE is_physical AND NOT (chain = 'SHUFERSAL' AND sub_chain_id = '5')""").first()[0]
assert len(base_pd) == n_scope, f"{len(base_pd)} rows, {n_scope} in-scope stores"
assert not base_pd.duplicated(["chain", "store_id"]).any(), "repeated chain + store_id"

# Signal 1 -------------------------------------------------------------------------------------------------
MIN_DAYS, SUSPECT_RATIO, GAP_RATIO, MIN_SIDE = 5, 0.25, 1.25, 3
base_pd["median_products"] = base_pd["median_products"].astype(float)
base_pd["chain_ratio"] = np.nan
base_pd["size_by_count"] = None
base_pd["size_by_count_reason"] = None

def fmt_k(x):
    return f"{x / 1000:.1f}k"

for chain, grp in base_pd.groupby("chain"):
    few = grp.index[(grp["days_observed"] < MIN_DAYS) | grp["median_products"].isna()]
    base_pd.loc[few, "size_by_count_reason"] = f"fewer than {MIN_DAYS} days of files"
    elig = grp.drop(index=few)
    if len(elig) == 0:
        continue
    p75 = np.percentile(elig["median_products"], 75)
    base_pd.loc[elig.index, "chain_ratio"] = elig["median_products"] / p75 if p75 > 0 else np.nan
    susp = elig.index[base_pd.loc[elig.index, "chain_ratio"] < SUSPECT_RATIO]
    base_pd.loc[susp, "size_by_count"] = "suspect"
    base_pd.loc[susp, "size_by_count_reason"] = f"ratio < {SUSPECT_RATIO}"
    rest = base_pd.loc[elig.index.difference(susp)].sort_values("chain_ratio")
    n = len(rest)
    if n < 2:
        base_pd.loc[rest.index, "size_by_count_reason"] = f"fewer than {2 * MIN_SIDE} stores to look for a gap"
        continue
    r = rest["chain_ratio"].to_numpy()
    m = rest["median_products"].to_numpy()
    jumps = r[1:] / r[:-1]
    k = int(np.argmax(jumps)) + 1  # first index above the gap
    jump = jumps[k - 1]
    if jump >= GAP_RATIO and k >= MIN_SIDE and n - k >= MIN_SIDE:
        base_pd.loc[rest.index[:k], "size_by_count"] = "small"
        base_pd.loc[rest.index[k:], "size_by_count"] = "full"
        base_pd.loc[rest.index, "size_by_count_reason"] = f"gap {fmt_k(m[k-1])} -> {fmt_k(m[k])} (x{jump:.2f})"
    elif jump >= GAP_RATIO:
        base_pd.loc[rest.index, "size_by_count_reason"] = (
            f"largest jump x{jump:.2f} ({fmt_k(m[k-1])} -> {fmt_k(m[k])}) has fewer than {MIN_SIDE} stores on a side")
    else:
        base_pd.loc[rest.index, "size_by_count_reason"] = f"no gap >= x{GAP_RATIO} in chain (largest x{jump:.2f})"

# Signal 2 -------------------------------------------------------------------------------------------------
# Whole words: the keyword must not touch a letter or digit on either side. Type order: industrial first;
# within a type the longest keyword first. Other matching types are listed in location_other_matches.
TYPE_ORDER = ["industrial", "mall", "commercial_center"]
patterns = []
for _, row in kw_pd.iterrows():
    kw = str(row["keyword"]).strip()
    patterns.append((TYPE_ORDER.index(row["location_type"]), -len(kw), kw, row["location_type"],
                     re.compile(r"(?<!\w)" + re.escape(kw) + r"(?!\w)", re.IGNORECASE)))
patterns.sort(key=lambda t: (t[0], t[1]))

def match_location(name, address):
    text = f"{name or ''} | {address or ''}"
    hits = [(typ, kw) for _, _, kw, typ, rx in patterns if rx.search(text)]
    if not hits:
        return None, None, None
    others = sorted({f"{typ}:{kw}" for typ, kw in hits[1:] if typ != hits[0][0]})
    return hits[0][0], hits[0][1], ", ".join(others) if others else None

loc = [match_location(a, b) for a, b in zip(base_pd["store_name"], base_pd["address"])]
base_pd["location_type"] = [x[0] for x in loc]
base_pd["location_keyword"] = [x[1] for x in loc]
base_pd["location_other_matches"] = [x[2] for x in loc]

# Chain's own label (comparison only) ----------------------------------------------------------------------
def format_label(row):
    c = row["chain"]
    if c == "SHUFERSAL" or c.startswith("MAHSANI"):
        v = row["sub_chain_name"]
        return v if isinstance(v, str) and v.strip() else None
    if c == "YAYNO_BITAN_AND_CARREFOUR":
        for w in ("היפר", "מרקט", "סיטי"):
            if w in (row["store_name"] or ""):
                return w
        return None
    if c == "RAMI_LEVY":
        try:
            return "רמי לוי בשכונה" if 700 <= int(row["store_id"]) <= 799 else "רמי לוי"
        except ValueError:
            return "רמי לוי"
    return None

SMALL_LABELS = {"שופרסל אקספרס", "יש Good", "יש בשכונה", "ישבשכונה", "סיטי", "רמי לוי בשכונה"}
base_pd["format_label"] = base_pd.apply(format_label, axis=1)
base_pd["label_size"] = base_pd["format_label"].map(
    lambda v: None if v is None else ("small" if v in SMALL_LABELS else "full"))

base_pd = base_pd.astype(object).where(base_pd.notna(), None)
cols = ["chain", "store_id", "store_name", "address", "days_observed", "median_products", "chain_ratio",
        "size_by_count", "size_by_count_reason", "location_type", "location_keyword", "location_other_matches",
        "format_label", "label_size"]
sig_df = spark.createDataFrame(
    [tuple(r) for r in base_pd[cols].itertuples(index=False)],
    "chain STRING, store_id STRING, store_name STRING, address STRING, days_observed INT, median_products DOUBLE, "
    "chain_ratio DOUBLE, size_by_count STRING, size_by_count_reason STRING, location_type STRING, "
    "location_keyword STRING, location_other_matches STRING, format_label STRING, label_size STRING")
sig_df.createOrReplaceTempView("store_size_signals_new")
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.store_size_signals
COMMENT 'Helper for dim_store: two store-size signals per physical store (Shufersal Be excluded): product count vs the chain (30-day median, gap rule) and location keywords, plus the chains own format label for comparison. Decides nothing.'
AS SELECT * FROM store_size_signals_new
""")

# COMMAND ----------

n, distinct_n = spark.sql(
    "SELECT count(*), count(DISTINCT chain, store_id) FROM workspace.gold.store_size_signals").first()
assert n == distinct_n, f"store_size_signals has repeated keys: {n} rows, {distinct_n} distinct"
assert n == n_scope, f"store_size_signals has {n} rows, {n_scope} in-scope stores"
print(f"store_size_signals: {n} rows")
