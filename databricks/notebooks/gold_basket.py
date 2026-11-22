# Databricks notebook source
# Gold, step G2: city and region price index. Reads only gold dims + silver; no price cleaning here.
#   gold.basket_items           TABLE  FROZEN fixed basket (up to 25 best-covered real barcodes per CBS category)
#   gold.fact_price_snapshot    TABLE  per open physical store x barcode: median shelf price, last 7 scraper days (rebuilt every run)
#   gold.v_store_price_index    VIEW   per store: store_index (vs the national price) and chain_index (vs the chain's own price)
#   gold.v_city_price_index     VIEW   per city, stores with enough coverage
#   gold.v_district_price_index VIEW   per district (a second view, not a level column)
# The snapshot lives in this notebook, not in gold_facts.py: it depends on dim_store is_open and only this step uses it.
# Shelf price = silver.price_obs.price, source = 'scraper' (no promotions).

dbutils.widgets.text("rebuild_basket", "no")
rebuild_basket = dbutils.widgets.get("rebuild_basket").strip().lower() == "yes"
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
BASKET = "staples_v1"

# COMMAND ----------

# Seed: databricks/seed/basket_category_weights.csv (CBS CPI weights, January 2025, 2022 household expenditure survey;
# table 1 of CBS release 043/2025). The bundle syncs seed/ next to the notebooks.
import pandas as pd
w = pd.read_csv("../seed/basket_category_weights.csv")
assert len(w) > 0 and w.basket_name.eq(BASKET).all(), "seed is empty or has another basket name"
spark.createDataFrame(w).createOrReplaceTempView("basket_weights_raw")
spark.sql("""
CREATE OR REPLACE TEMP VIEW basket_weights AS
SELECT basket_name, category, CAST(cbs_weight AS DOUBLE) AS cbs_weight, cbs_groups FROM basket_weights_raw
""")

# COMMAND ----------

# gold.basket_items: FROZEN. A basket that changed every day would break comparisons over time, so it is built only
# when the table does not exist, or when widget rebuild_basket = yes (then the whole basket is replaced).
# Selection on the latest scraper file_date: real barcodes (is_gs1_barcode, shared), carried by >= 8 distinct chains
# and by >= 50% of the open in-scope stores that day; per category in the seed, the top 25 by store coverage
# (ties: more chains, then item_code).
# Category: for now silver.product_categories_ai (model databricks-gpt-oss-120b), joined on item_key.
# When the final table is ready, switch to dim_product.category: change only the CATEGORY_SOURCE block below
# (workspace.gold.dim_product, product_key AS item_key, category).
exists = spark.catalog.tableExists("workspace.gold.basket_items")
if exists and not rebuild_basket:
    print("gold.basket_items exists and rebuild_basket is not 'yes': basket left as it is")
else:
    spark.sql(f"""
    CREATE OR REPLACE TABLE workspace.gold.basket_items
    COMMENT 'FROZEN fixed basket {BASKET}: up to 25 best-covered real barcodes per CBS category, chosen on one day. Rebuilt only with notebook parameter rebuild_basket=yes.'
    AS
    WITH last_day AS (
      SELECT max(file_date) AS d FROM workspace.silver.price_obs WHERE source = 'scraper'
    ),
    open_stores AS (SELECT chain, store_id, store_key FROM workspace.gold.dim_store WHERE is_open),
    n_stores AS (SELECT count(*) AS n FROM open_stores),
    cand AS (
      SELECT p.item_code,
             count(DISTINCT s.store_key) AS stores_on_selection_day,
             count(DISTINCT p.chain)     AS chains_on_selection_day
      FROM workspace.silver.price_obs p
      JOIN last_day ld ON p.file_date = ld.d
      JOIN open_stores s ON s.chain = p.chain AND s.store_id = p.store_id
      JOIN workspace.silver.items i ON i.item_key = p.item_code AND i.item_scope = 'shared' AND i.is_gs1_barcode
      WHERE p.source = 'scraper'
      GROUP BY p.item_code
      HAVING count(DISTINCT p.chain) >= 8
         AND count(DISTINCT s.store_key) >= 0.5 * (SELECT n FROM n_stores)
    ),
    cat AS (
      -- CATEGORY_SOURCE
      SELECT item_key, category FROM workspace.silver.product_categories_ai
      WHERE model = 'databricks-gpt-oss-120b'
    ),
    ranked AS (
      SELECT c.*, i.item_key, i.item_name, k.category, wt.cbs_weight,
             row_number() OVER (PARTITION BY k.category
                                ORDER BY c.stores_on_selection_day DESC, c.chains_on_selection_day DESC, c.item_code) AS rank_in_category
      FROM cand c
      JOIN workspace.silver.items i ON i.item_key = c.item_code AND i.item_scope = 'shared'
      JOIN cat k ON k.item_key = i.item_key
      JOIN basket_weights wt ON wt.category = k.category
    )
    SELECT
      '{BASKET}'                 AS basket_name,
      item_key,
      item_code,
      item_name                  AS product_name,
      category,
      cbs_weight                 AS category_weight,
      stores_on_selection_day,
      chains_on_selection_day,
      rank_in_category,
      (SELECT d FROM last_day)   AS selected_on,
      'gs1 shared barcode; >= 8 chains and >= 50% of open in-scope stores on selection day; top 25 per category by stores, chains, item_code' AS selection_rule
    FROM ranked
    WHERE rank_in_category <= 25
    """)
display(spark.sql("SELECT category, count(*) AS products FROM workspace.gold.basket_items GROUP BY category ORDER BY category"))
n_basket = spark.sql("SELECT count(*) FROM workspace.gold.basket_items").first()[0]
assert n_basket > 0, "basket is empty"
print("basket rows:", n_basket)

# COMMAND ----------

# gold.fact_price_snapshot: one row per open in-scope store x real shared barcode. Median shelf price over the last 7
# distinct scraper file_dates; the window ends at the latest scraper file_date. Not basket-specific: any cross-chain
# basket can read it. Rebuilt in every run.
spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.fact_price_snapshot
CLUSTER BY (item_key, store_key)
COMMENT 'Per open physical store x real shared barcode: median shelf price over the last 7 scraper file dates (window ends at snapshot_end_date). Rebuilt every run.'
AS
WITH win AS (
  SELECT min(file_date) AS start_date, max(file_date) AS end_date
  FROM (SELECT DISTINCT file_date FROM workspace.silver.price_obs WHERE source = 'scraper'
        ORDER BY file_date DESC LIMIT 7)
)
SELECT
  p.chain,
  p.store_id,
  s.store_key,
  i.item_key,
  p.item_code,
  CAST(median(p.price) AS DECIMAL(10,2))  AS median_price,
  count(DISTINCT p.file_date)             AS days_observed,
  (SELECT end_date FROM win)              AS snapshot_end_date
FROM workspace.silver.price_obs p
JOIN workspace.gold.dim_store s ON s.chain = p.chain AND s.store_id = p.store_id AND s.is_open
JOIN workspace.silver.items i ON i.item_key = p.item_code AND i.item_scope = 'shared' AND i.is_gs1_barcode
WHERE p.source = 'scraper'
  AND p.file_date BETWEEN (SELECT start_date FROM win) AND (SELECT end_date FROM win)
GROUP BY p.chain, p.store_id, s.store_key, i.item_key, p.item_code
""")
n, d = spark.sql("SELECT count(*), count(DISTINCT store_key) FROM workspace.gold.fact_price_snapshot").first()
print(f"fact_price_snapshot: {n} rows, {d} stores")
assert n > 0, "snapshot is empty"

# COMMAND ----------

# v_store_price_index: for each basket product at a store, ratio = store median price / national median of that product
# (over all in-scope open stores in the snapshot); category index = median ratio over the store's basket products in
# the category; store_index = sum(weight x category index) / sum(weight over categories the store has).
# chain_index: the same with the CHAIN's median of the product as the reference (does the chain charge more here than
# in its other stores). weight_covered = share of the whole basket weight the store has.
spark.sql(f"""
CREATE OR REPLACE VIEW workspace.gold.v_store_price_index
COMMENT 'Per open store: price index of the staples_v1 basket. store_index vs the national median price, chain_index vs the chain median. low_coverage = weight_covered < 0.6 or products_priced < 50.'
AS
WITH b AS (
  SELECT item_key, category, category_weight FROM workspace.gold.basket_items WHERE basket_name = '{BASKET}'
),
total_w AS (SELECT sum(w) AS w FROM (SELECT category, max(category_weight) AS w FROM b GROUP BY category)),
p AS (
  SELECT f.store_key, f.chain, f.item_key, CAST(f.median_price AS DOUBLE) AS price, b.category, b.category_weight
  FROM workspace.gold.fact_price_snapshot f JOIN b ON b.item_key = f.item_key
),
nat AS (SELECT item_key, median(price) AS nat_med FROM p GROUP BY item_key),
chn AS (SELECT item_key, chain, median(price) AS chain_med FROM p GROUP BY item_key, chain),
r AS (
  SELECT p.store_key, p.category, p.category_weight,
         p.price / nat.nat_med   AS ratio,
         p.price / chn.chain_med AS chain_ratio
  FROM p JOIN nat ON nat.item_key = p.item_key
         JOIN chn ON chn.item_key = p.item_key AND chn.chain = p.chain
),
cat AS (
  SELECT store_key, category, max(category_weight) AS w,
         median(ratio) AS cat_index, median(chain_ratio) AS cat_chain_index, count(*) AS products
  FROM r GROUP BY store_key, category
),
st AS (
  SELECT store_key,
         sum(w * cat_index) / sum(w)        AS store_index,
         sum(w * cat_chain_index) / sum(w)  AS chain_index,
         sum(products)                      AS products_priced,
         count(*)                           AS categories_priced,
         sum(w)                             AS w_have
  FROM cat GROUP BY store_key
)
SELECT
  d.store_key, d.chain, d.store_id, d.store_name, d.city_code, d.city_name, d.district, d.size_bucket,
  st.store_index, st.chain_index, st.products_priced, st.categories_priced,
  st.w_have / (SELECT w FROM total_w) AS weight_covered,
  (st.w_have / (SELECT w FROM total_w) < 0.6 OR st.products_priced < 50) AS low_coverage
FROM st JOIN workspace.gold.dim_store d ON d.store_key = st.store_key
""")

# COMMAND ----------

# City and district views: only stores with low_coverage = false. A: avg store_index (where is it cheap to shop, the
# chain mix included). B: avg chain_index (does the chain charge more here than elsewhere). few_stores = fewer than 3
# stores. Two views, one per level.
for level, cols, name in [("city", "city_code, city_name, district", "v_city_price_index"),
                          ("district", "district", "v_district_price_index")]:
    spark.sql(f"""
    CREATE OR REPLACE VIEW workspace.gold.{name}
    COMMENT 'Price index of the staples_v1 basket per {level}, stores with low_coverage = false only. avg_store_index = A (where is it cheap to shop, chain mix included); avg_chain_index = B (chain pricing by place). few_stores = fewer than 3 stores.'
    AS
    SELECT {cols},
           count(*)                  AS stores,
           count(DISTINCT chain)     AS chains_present,
           avg(store_index)          AS avg_store_index,
           avg(chain_index)          AS avg_chain_index,
           count(*) < 3              AS few_stores
    FROM workspace.gold.v_store_price_index
    WHERE NOT low_coverage
    GROUP BY {cols}
    """)
print(spark.sql("SELECT count(*) AS stores, count_if(low_coverage) AS low_cov, median(store_index) AS median_index FROM workspace.gold.v_store_price_index").first())
