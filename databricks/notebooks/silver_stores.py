# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# COMMAND ----------

# Step 1: the stores as published, in a temp view (the final table is written in the last cell, after the city
# is resolved, so a failure in the city step leaves the old table untouched).
spark.sql("""
CREATE OR REPLACE TEMP VIEW stores_base AS
WITH parsed AS (
  SELECT
    chain,
    coalesce(header['ChainID'], header['ChainId']) AS chain_id,
    coalesce(header['SubChainID'], header['SubChainId']) AS sub_chain_id,
    fields['StoreID'] AS store_id_raw,
    regexp_replace(fields['StoreID'], '^0+(?=.)', '') AS store_id,
    fields['StoreName'] AS store_name,
    fields['Address'] AS address,
    nullif(trim(fields['City']), '') AS city_raw,
    nullif(regexp_replace(coalesce(fields['ZIPCode'], fields['ZipCode']), '^0+$', ''), '') AS zip_code,
    fields['StoreType'] AS store_type,
    to_date(regexp_extract(file_name, '-(20[0-9]{6})', 1), 'yyyyMMdd') AS file_date,
    modified_at,
    file_path
  FROM workspace.bronze.stores
  -- the current store list comes from our own collection only; Kaggle store files (history) stay in bronze
  WHERE parse_error IS NULL AND source = 'scraper'
),
latest AS (
  SELECT *,
    row_number() OVER (PARTITION BY chain, store_id ORDER BY file_date DESC, modified_at DESC) AS rn
  FROM parsed
),
flagged AS (
  SELECT *, coalesce(store_type = '1', false) AS is_physical
  FROM latest
  WHERE rn = 1
)
SELECT
  chain,
  chain_id,
  sub_chain_id,
  store_id,
  store_id_raw,
  store_name,
  address,
  CASE WHEN city_raw RLIKE '^[1-9][0-9]*$' THEN CAST(city_raw AS INT) END AS city_code,
  CASE WHEN city_raw RLIKE '[\u0590-\u05FF]' THEN city_raw END AS city_name,
  zip_code,
  store_type,
  is_physical,
  is_physical AND coalesce(city_raw IN ('2600', '\u05D0\u05D9\u05DC\u05EA') OR zip_code LIKE '88%', false) AS is_eilat,
  file_date AS last_file_date,
  file_path AS source_file
FROM flagged
""")

# COMMAND ----------

# Step 2: resolve the city (G1b-1). Seeds, read relative to this notebook like in gold_dims.py:
#   ../seed/cbs_localities.csv  Central Bureau of Statistics locality list (code, Hebrew + English name, district)
#   ../seed/city_aliases.csv    match_type name = another spelling -> code; match_type code = a store's wrong code -> right code
# Order per store, first hit wins:
#   1 code        raw numeric city code (not 0) is in the CBS list
#   2 code_alias  the code is in the code aliases
#   3 name        city_name text, normalized, equals a CBS name or a name alias
#   4 store_name  a locality name (3+ letters, longest first) found as whole words in store_name
#   5 address     same in address
#   else none
# A few thousand names at most, ~1k stores: plain pandas on the driver.
import re
import pandas as pd

STOP_NAME = {"אזור", "רחוב", "מרכז", "כרמים"}
STOP_ADDR = STOP_NAME | {"שדרות", "מבקיעים"}
_PUNCT = re.compile("[-–־\"״'׳`.,()]")  # hyphens, Hebrew maqaf, quotes, geresh, dots, commas, brackets

def norm_city(s):
    s = "" if s is None or (isinstance(s, float) and s != s) else str(s)
    s = _PUNCT.sub(" ", s)
    s = s.replace("יי", "י").replace("וו", "ו")
    return re.sub(r"\s+", " ", s).strip()

def build_city_resolver(cbs_pd, alias_pd):
    by_code = dict(zip(cbs_pd["city_code"], cbs_pd["city_name"]))
    code_alias = {}
    names = list(zip(cbs_pd["city_name"], cbs_pd["city_code"]))
    for mt, val, code in zip(alias_pd["match_type"], alias_pd["value"], alias_pd["city_code"]):
        assert code in by_code, f"city_aliases.csv: code {code} is not in cbs_localities.csv"
        if mt == "code":
            code_alias[val] = code
        else:
            names.append((val, code))
    by_name = {norm_city(n).replace(" ", ""): c for n, c in names}
    word_list = sorted(((norm_city(n), c) for n, c in names if len(norm_city(n)) >= 3), key=lambda t: -len(t[0]))

    def find(text, stop):
        t = " " + norm_city(text) + " "
        for n, c in word_list:
            if n not in stop and (" " + n + " ") in t:
                return c
        return None

    def resolve(city_code, city_name, store_name, address):
        cc = str(int(city_code)) if city_code is not None and city_code == city_code and int(city_code) != 0 else None
        if cc and cc in by_code:
            return cc, "code"
        if cc and cc in code_alias:
            return code_alias[cc], "code_alias"
        if city_name:
            c = by_name.get(norm_city(city_name).replace(" ", ""))
            if c:
                return c, "name"
        c = find(store_name, STOP_NAME)
        if c:
            return c, "store_name"
        c = find(address, STOP_ADDR)
        if c:
            return c, "address"
        return None, "none"

    return by_code, resolve

# COMMAND ----------

cbs_pd = pd.read_csv("../seed/cbs_localities.csv", dtype=str, keep_default_na=False, encoding="utf-8-sig")
alias_pd = pd.read_csv("../seed/city_aliases.csv", dtype=str, keep_default_na=False, encoding="utf-8-sig")
assert len(cbs_pd) > 1000 and cbs_pd["city_code"].is_unique, "bad cbs_localities.csv"
by_code, resolve_city = build_city_resolver(cbs_pd, alias_pd)

stores_pd = spark.table("stores_base").select("chain", "store_id", "store_name", "address", "city_code", "city_name").toPandas()
res = [resolve_city(c if pd.notna(c) else None, n, sn, ad)
       for c, n, sn, ad in zip(stores_pd["city_code"], stores_pd["city_name"], stores_pd["store_name"], stores_pd["address"])]
stores_pd["city_code_resolved"] = [int(c) if c else None for c, _ in res]
stores_pd["city_source"] = [s for _, s in res]
resolved_pd = stores_pd[["chain", "store_id", "city_code_resolved", "city_source"]].astype({"city_code_resolved": "object"})
resolved_pd = resolved_pd.where(resolved_pd.notna(), None)
spark.createDataFrame(
    [tuple(r) for r in resolved_pd.itertuples(index=False)],
    "chain STRING, store_id STRING, city_code_resolved INT, city_source STRING",
).createOrReplaceTempView("stores_city")

cbs_df = cbs_pd.assign(city_code_resolved=cbs_pd["city_code"].astype(int))[
    ["city_code_resolved", "city_name", "city_name_en", "district_name"]
].rename(columns={"city_name": "city_name_resolved"})
spark.createDataFrame(cbs_df).createOrReplaceTempView("cbs_localities")

# COMMAND ----------

# Step 3: the final table. city_code / city_name stay as published; the *_resolved columns are the ones to use.
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.stores AS
SELECT
  b.chain, b.chain_id, b.sub_chain_id, b.store_id, b.store_id_raw, b.store_name, b.address,
  b.city_code, b.city_name,
  r.city_code_resolved,
  c.city_name_resolved,
  c.city_name_en,
  c.district_name,
  r.city_source,
  b.zip_code, b.store_type, b.is_physical, b.is_eilat, b.last_file_date, b.source_file
FROM stores_base b
LEFT JOIN stores_city r ON r.chain = b.chain AND r.store_id = b.store_id
LEFT JOIN cbs_localities c ON c.city_code_resolved = r.city_code_resolved
""")

# COMMAND ----------

n, distinct_n = spark.sql("SELECT count(*), count(DISTINCT chain, store_id) FROM workspace.silver.stores").first()
assert n == distinct_n, f"silver.stores has repeated keys: {n} rows, {distinct_n} distinct"
print(f"silver.stores: {n} rows")
spark.sql("SELECT city_source, count(*) AS stores FROM workspace.silver.stores GROUP BY city_source ORDER BY stores DESC").show()
# Printed comparison, no assert: our Eilat rule against the resolved city code
spark.sql("""
SELECT is_eilat, coalesce(city_code_resolved = 2600, false) AS resolved_2600, count(*) AS stores
FROM workspace.silver.stores WHERE is_physical GROUP BY 1, 2 ORDER BY 1, 2
""").show()
