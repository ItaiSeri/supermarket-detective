# Databricks notebook source
# silver.store_size_signals (G1b-2a v2): two cheap signals of how big each store is, saved for review. A helper
# table for gold.dim_store, not for Power BI. size_bucket / size_rule / size_basis hold the final size.
# Scope: physical stores except Shufersal Be (sub_chain_id 5, out of gold). One row per chain + store_id.
#  Signal 1: median over the last 30 days of the number of price rows per file_date (needs 5+ days of files),
#            compared inside the chain with a valid-split gap rule (below). chain_ratio (vs the chain's 75th
#            percentile) is information only.
#  Signal 2: location keywords (seed/store_location_keywords.csv) found as whole words in store_name or address.
#  format_label / label_size: the chain's own format, for comparison only.
#  store_format_flag: 'produce' for Hazi Hinam produce stores (flagged, kept, left out of the gap search).
# It moved here from gold_dims.py (it reads only silver tables); the old gold table is dropped.
import re
import numpy as np
import pandas as pd

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("DROP TABLE IF EXISTS workspace.gold.store_size_signals")

# COMMAND ----------

kw_pd = pd.read_csv("../seed/store_location_keywords.csv")
assert len(kw_pd) > 0 and set(kw_pd.columns) == {"keyword", "location_type"}, "bad keyword seed"

base_pd = spark.sql("""
WITH scope AS (
  SELECT chain, store_id, store_name, address, city_name_resolved, district_name, source_file
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
SELECT s.chain, s.store_id, s.store_name, s.address, s.city_name_resolved, s.district_name,
       coalesce(g.days_observed, 0) AS days_observed, g.median_products, l.sub_chain_name
FROM scope s
LEFT JOIN sig g ON g.chain = s.chain AND g.store_id = s.store_id
LEFT JOIN labels l ON l.chain = s.chain AND l.store_id = s.store_id
""").toPandas()
n_scope = spark.sql("""SELECT count(*) FROM workspace.silver.stores
  WHERE is_physical AND NOT (chain = 'SHUFERSAL' AND sub_chain_id = '5')""").first()[0]
assert len(base_pd) == n_scope, f"{len(base_pd)} rows, {n_scope} in-scope stores"
assert not base_pd.duplicated(["chain", "store_id"]).any(), "repeated chain + store_id"

# COMMAND ----------

# Signal 1 and the produce flag. Rules:
#  - suspect: fewer than MIN_DAYS days of files, or no price rows at all (probably incomplete or new). Nothing else
#    makes a store suspect; a low product count alone does not.
#  - produce: Hazi Hinam stores whose name contains the produce words. Flagged, kept, left out of the gap search
#    and of the chain's 75th percentile.
#  - gap search, per chain, over the remaining stores (MIN_DAYS or more days): sort by median_products; only splits
#    with at least MIN_SIDE stores on each side count; the split with the largest ratio (upper neighbour / lower
#    neighbour) wins. If that ratio is at least GAP_RATIO the stores below are 'small' and those above 'large';
#    otherwise NULL, with the largest valid ratio in the reason.
MIN_DAYS, GAP_RATIO, MIN_SIDE = 5, 1.25, 3
PRODUCE_WORDS = "תוצרת חקלאית"  # produce-store words in the name


def fmt_k(x):
    return f"{x / 1000:.1f}k"


def apply_size_by_count(df):
    df = df.copy()
    df["median_products"] = df["median_products"].astype(float)
    df["chain_ratio"] = np.nan
    df["size_by_count"] = None
    df["size_by_count_reason"] = None
    df["store_format_flag"] = None
    produce = (df["chain"] == "HAZI_HINAM") & df["store_name"].fillna("").str.contains(PRODUCE_WORDS, regex=False)
    df.loc[produce, "store_format_flag"] = "produce"
    few = (df["days_observed"] < MIN_DAYS) | df["median_products"].isna()
    df.loc[few, "size_by_count"] = "suspect"
    df.loc[few, "size_by_count_reason"] = f"fewer than {MIN_DAYS} days of files"
    df.loc[produce & ~few, "size_by_count_reason"] = "produce store, left out of the gap search"
    for chain, grp in df[~few & ~produce].groupby("chain"):
        p75 = np.percentile(grp["median_products"], 75)
        if p75 > 0:
            df.loc[grp.index, "chain_ratio"] = grp["median_products"] / p75
        rest = grp.sort_values("median_products")
        n = len(rest)
        m = rest["median_products"].to_numpy()
        if n < 2 * MIN_SIDE:
            df.loc[rest.index, "size_by_count_reason"] = f"fewer than {2 * MIN_SIDE} stores to look for a gap"
            continue
        best_k, best_r = None, -1.0
        for k in range(MIN_SIDE, n - MIN_SIDE + 1):  # k stores below the split, n - k above
            r = m[k] / m[k - 1] if m[k - 1] > 0 else np.inf
            if r > best_r:
                best_k, best_r = k, r
        if best_r >= GAP_RATIO:
            df.loc[rest.index[:best_k], "size_by_count"] = "small"
            df.loc[rest.index[best_k:], "size_by_count"] = "large"
            df.loc[rest.index, "size_by_count_reason"] = (
                f"split {fmt_k(m[best_k - 1])} -> {fmt_k(m[best_k])} (x{best_r:.2f})")
        else:
            df.loc[rest.index, "size_by_count_reason"] = f"no gap >= x{GAP_RATIO} (largest valid x{best_r:.2f})"
    return df


base_pd = apply_size_by_count(base_pd)

# COMMAND ----------

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
# Hebrew labels are written as \u escapes: HYPER = היפר, MARKET = מרקט, CITY = סיטי,
# RAMI = רמי לוי, RAMI_NEIGHBOURHOOD = רמי לוי בשכונה
HYPER, MARKET, CITY = "היפר", "מרקט", "סיטי"
RAMI = "רמי לוי"
RAMI_NEIGHBOURHOOD = RAMI + " בשכונה"


def format_label(row):
    c = row["chain"]
    if c == "SHUFERSAL" or c.startswith("MAHSANI"):
        v = row["sub_chain_name"]
        return v if isinstance(v, str) and v.strip() else None
    if c == "YAYNO_BITAN_AND_CARREFOUR":
        for w in (HYPER, MARKET, CITY):
            if w in (row["store_name"] or ""):
                return w
        return None
    if c == "RAMI_LEVY":
        try:
            return RAMI_NEIGHBOURHOOD if 700 <= int(row["store_id"]) <= 799 else RAMI
        except ValueError:
            return RAMI
    return None


# small formats: Shufersal Express, Yesh Good, Yesh Bishchuna (two spellings), Carrefour City, Rami Levy Bishchuna
SMALL_LABELS = {
    "שופרסל אקספרס",
    "יש Good",
    "יש בשכונה",
    "ישבשכונה",
    CITY,
    RAMI_NEIGHBOURHOOD,
}
base_pd["format_label"] = base_pd.apply(format_label, axis=1)
base_pd["label_size"] = base_pd["format_label"].map(
    lambda v: None if v is None else ("small" if v in SMALL_LABELS else "full"))

# COMMAND ----------

# Final size bucket (decided by Itai 2026-11-19). Ordered rules per chain, first match wins; n = median_products.
# size_basis says what decided it: format (the chain's own label), count (product count), chain_default,
# location (keywords), no_data. The size_by_count / label_size columns stay as diagnostics.
POWER_WORDS = {"פאוור סנטר", "פאואר סנטר", "ביג", "big"}
STEP_UP = {"small": "medium", "medium": "large"}


def is_power_center(row):
    words = [str(row["location_keyword"] or "")] + [
        x.split(":", 1)[-1] for x in str(row["location_other_matches"] or "").split(", ") if x]
    return any(w.lower() in POWER_WORDS for w in words)


def by_count(n, large, medium):
    return "large" if n >= large else ("medium" if n >= medium else "small")


def size_bucket(row):
    c, f, n = row["chain"], row["format_label"], row["median_products"]
    no_n = n is None or pd.isna(n)
    loc = row["location_type"]
    if c == "OSHER_AD":  # R_OSHER: every Osher Ad store is a large store
        return "large", "R_OSHER", "chain_default"
    if c == "HAZI_HINAM":
        if row["store_format_flag"] == "produce":  # R_PRODUCE: produce-only stores get no bucket
            return None, "R_PRODUCE", "chain_default"
        return "large", "R_HAZI", "chain_default"  # R_HAZI: all other Hazi Hinam stores are large
    if c == "SHUFERSAL":
        if f == "יוניברס":
            return "large", "R_SHUF_UNIVERSE", "format"
        if f in ("שופרסל אקספרס", "יש Good"):
            return "small", "R_SHUF_SMALL", "format"
        if f == "שופרסל דיל":  # Deal: large if industrial zone or power center, else medium
            if loc == "industrial" or is_power_center(row):
                return "large", "R_SHUF_DEAL_LOC", "location"
            return "medium", "R_SHUF_DEAL", "format"
        if f in ("שופרסל שלי", "יש חסד", "שערי רווחה", "Cash&Carry"):
            return "medium", "R_SHUF_MEDIUM", "format"
        if f == "ישבשכונה":
            return "small", "R_SHUF_NEIGHBOURHOOD", "format"
        return "medium", "R_SHUF_DEFAULT", "chain_default"
    if c == "RAMI_LEVY":
        if f == RAMI_NEIGHBOURHOOD:  # R_RAMI_BESHCHUNA
            return "small", "R_RAMI_BESHCHUNA", "format"
        if no_n:
            return None, "R_RAMI_NO_DATA", "no_data"
        return ("large" if n >= 13000 else "medium"), "R_RAMI_COUNT", "count"
    if c == "YAYNO_BITAN_AND_CARREFOUR":
        if f in (HYPER, MARKET, CITY):  # the chain's own format word in the name
            return {HYPER: "large", MARKET: "medium", CITY: "small"}[f], "R_CARREFOUR_FORMAT", "format"
        if no_n:
            return None, "R_CARREFOUR_NO_DATA", "no_data"
        b = by_count(n, 10500, 8300)
        if loc == "industrial" and b in STEP_UP:  # industrial zone: one step up
            return STEP_UP[b], "R_CARREFOUR_COUNT_INDUSTRIAL", "location"
        return b, "R_CARREFOUR_COUNT", "count"
    if c == "VICTORY_NEW_SOURCE":
        if CITY in (row["store_name"] or ""):
            return "small", "R_VICTORY_CITY", "format"
        return "medium", "R_VICTORY_DEFAULT", "chain_default"
    if c == "MAHSANI_ASHUK_NEW_SOURCE":
        m = {"מחסני השוק": "large", "מחסני השוק בשבילך": "medium",
             "מחסני השוק בסיטי": "small", "מחסני השוק מהדרין": "medium"}
        if f in m:
            return m[f], "R_MAHSANI_FORMAT", "format"
        return "medium", "R_MAHSANI_DEFAULT", "chain_default"
    if c == "YOHANANOF":
        if no_n:
            return None, "R_YOHANANOF_NO_DATA", "no_data"
        b = by_count(n, 9000, 6000)
        if loc == "industrial":
            return "large", "R_YOHANANOF_INDUSTRIAL", "location"
        if loc == "mall" and b == "large":
            return "medium", "R_YOHANANOF_MALL", "location"
        return b, "R_YOHANANOF_COUNT", "count"
    if c == "KESHET":
        name = (row["store_name"] or "").strip()
        # R_KESHET_SMALL_FORMAT: Kolinarik gourmet deli branches and the Azrieli food truck are small
        if name.startswith("קולינריק") or "פודטראק עזריאלי" in name:
            return "small", "R_KESHET_SMALL_FORMAT", "format"
        return "medium", "R_CHAIN_MEDIUM", "chain_default"
    if c == "TIV_TAAM":
        return "medium", "R_CHAIN_MEDIUM", "chain_default"
    return None, "R_UNMATCHED", "no_data"


_sb = base_pd.apply(size_bucket, axis=1, result_type="expand")
base_pd["size_bucket"], base_pd["size_rule"], base_pd["size_basis"] = _sb[0], _sb[1], _sb[2]
n_unmatched = int((base_pd["size_rule"] == "R_UNMATCHED").sum())
if n_unmatched:
    print(f"WARNING: {n_unmatched} stores matched no size rule (R_UNMATCHED)")

# COMMAND ----------

base_pd = base_pd.astype(object).where(base_pd.notna(), None)
cols = ["chain", "store_id", "store_name", "address", "city_name_resolved", "district_name",
        "days_observed", "median_products", "chain_ratio", "size_by_count", "size_by_count_reason",
        "store_format_flag", "location_type", "location_keyword", "location_other_matches",
        "format_label", "label_size", "size_bucket", "size_rule", "size_basis"]
sig_df = spark.createDataFrame(
    [tuple(r) for r in base_pd[cols].itertuples(index=False)],
    "chain STRING, store_id STRING, store_name STRING, address STRING, city_name_resolved STRING, "
    "district_name STRING, days_observed INT, median_products DOUBLE, chain_ratio DOUBLE, "
    "size_by_count STRING, size_by_count_reason STRING, store_format_flag STRING, location_type STRING, "
    "location_keyword STRING, location_other_matches STRING, format_label STRING, label_size STRING, size_bucket STRING, size_rule STRING, size_basis STRING")
sig_df.createOrReplaceTempView("store_size_signals_new")
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.store_size_signals
COMMENT 'Helper for gold.dim_store: two store-size signals per physical store (Shufersal Be excluded): product count within the chain (30-day median, valid-split gap rule; suspect = fewer than 5 days of files) and location keywords, plus the chains own format label, a produce flag and the resolved city. size_bucket (small, medium or large) is the final size by ordered rules per chain (rule id in size_rule, what decided it in size_basis); the other columns are diagnostics.'
AS SELECT * FROM store_size_signals_new
""")

# COMMAND ----------

n, distinct_n = spark.sql(
    "SELECT count(*), count(DISTINCT chain, store_id) FROM workspace.silver.store_size_signals").first()
assert n == distinct_n, f"store_size_signals has repeated keys: {n} rows, {distinct_n} distinct"
assert n == n_scope, f"store_size_signals has {n} rows, {n_scope} in-scope stores"
print(f"store_size_signals: {n} rows")
