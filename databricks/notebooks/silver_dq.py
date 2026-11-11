# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.dq_results (
  run_at TIMESTAMP,
  check_name STRING,
  check_level STRING,
  file_date DATE,
  observed DOUBLE,
  warn_above DOUBLE,
  fail_above DOUBLE,
  outcome STRING,
  meaning STRING
)
""")

# COMMAND ----------

P = "workspace.silver.price_obs"
Q = "workspace.silver.quarantine"
S = "workspace.silver.stores"
H = "workspace.silver.price_history"
I = "workspace.silver.items"
PO = "workspace.silver.promotion_obs"
PQ = "workspace.silver.promotion_quarantine"

latest = spark.sql(f"SELECT max(file_date) AS d FROM {P}").first()["d"]
previous = spark.sql(f"SELECT max(file_date) AS d FROM {P} WHERE file_date < DATE'{latest}'").first()["d"]
collected = spark.sql(f"SELECT max(collected_date) AS d FROM {P}").first()["d"]
run_at = spark.sql("SELECT current_timestamp() AS t").first()["t"]

promo_latest = spark.sql(f"SELECT max(file_date) AS d FROM {PO}").first()["d"]

day_stores = f"(SELECT DISTINCT chain, store_id FROM {P} WHERE file_date = DATE'{latest}')"

# Completeness: physical stores in each chain's newest Stores file (on or before the latest
# price date) against stores that have rows in price_obs on a date. Store IDs lose leading
# zeros the same way as in silver_stores and silver_price_obs.
listed_cte = f"""
sf AS (
  SELECT chain, regexp_replace(fields['StoreID'], '^0+(?=.)', '') AS store_id,
         to_date(regexp_extract(file_name, '-(20[0-9]{{6}})', 1), 'yyyyMMdd') AS d
  FROM workspace.bronze.stores WHERE parse_error IS NULL
),
newest AS (SELECT chain, max(d) AS d FROM sf WHERE d <= DATE'{latest}' GROUP BY chain),
listed AS (
  SELECT DISTINCT sf.chain, sf.store_id
  FROM sf JOIN newest n ON n.chain = sf.chain AND n.d = sf.d
  JOIN {S} s ON s.chain = sf.chain AND s.store_id = sf.store_id AND s.is_physical
)"""

# One row per chain and date in the last 7 days: stores listed, stores that published
pub_cte = f"""
nlisted AS (SELECT chain, count(*) AS n FROM listed GROUP BY chain),
daily AS (
  SELECT u.chain, u.file_date, count(DISTINCT u.store_id) AS published
  FROM {P} u JOIN listed l ON l.chain = u.chain AND l.store_id = u.store_id
  WHERE u.file_date BETWEEN DATE'{latest}' - 7 AND DATE'{latest}'
  GROUP BY u.chain, u.file_date
)"""

# Chains that published on the latest date, with missing stores now and in a normal
# recent day (median of the earlier days in the window on which the chain published)
completeness_gap = f"""
WITH {listed_cte}, {pub_cte},
today AS (
  SELECT d.chain, n.n AS listed, n.n - d.published AS missing
  FROM daily d JOIN nlisted n ON n.chain = d.chain WHERE d.file_date = DATE'{latest}'
),
base AS (
  SELECT d.chain, percentile_approx(n.n - d.published, 0.5) AS missing
  FROM daily d JOIN nlisted n ON n.chain = d.chain WHERE d.file_date < DATE'{latest}'
  GROUP BY d.chain
)
SELECT count_if(today.missing - coalesce(base.missing, today.missing) > greatest(2, 0.03 * today.listed))
FROM today LEFT JOIN base ON base.chain = today.chain"""

chains_without_files = f"""
WITH {listed_cte}
SELECT count(*) FROM (SELECT DISTINCT chain FROM listed) l
LEFT ANTI JOIN (SELECT DISTINCT chain FROM {P} WHERE file_date = DATE'{latest}') p ON p.chain = l.chain"""

# name, level, query returning one number, warn above, fail above, meaning
checks = [
    ("repeated_keys", "row",
     f"SELECT count(*) FROM (SELECT 1 FROM {P} WHERE file_date = DATE'{latest}' GROUP BY chain, store_id, item_code HAVING count(*) > 1)",
     None, 0, "Same chain, store, product and day appears more than once"),
    ("bad_prices", "row",
     f"SELECT count_if(price IS NULL OR price <= 0) FROM {P} WHERE file_date = DATE'{latest}'",
     None, 0, "A missing or non-positive price reached price_obs"),
    ("missing_keys", "row",
     f"SELECT count_if(store_id IS NULL OR item_code IS NULL) FROM {P} WHERE file_date = DATE'{latest}'",
     None, 0, "A row in price_obs has no store or product code"),
    ("reject_percent", "row",
     f"SELECT 100.0 * q.n / (q.n + p.n) FROM (SELECT count(*) AS n FROM {Q} WHERE file_date = DATE'{latest}') q, (SELECT count(*) AS n FROM {P} WHERE file_date = DATE'{latest}') p",
     0.1, 5, "Percent of the latest day's rows that were rejected"),
    ("files_not_readable", "file",
     f"SELECT count(DISTINCT source_file) FROM {Q} WHERE reject_reason = 'file_not_readable' AND collected_date = DATE'{collected}'",
     0, None, "Files from the latest collection that could not be parsed"),
    ("wrong_chain_rows", "file",
     f"SELECT count(*) FROM {Q} WHERE reject_reason = 'wrong_chain' AND collected_date = DATE'{collected}'",
     0, None, "Rows in a chain's folder that carry another chain's ID"),
    ("stores_without_record", "business",
     f"SELECT count(*) FROM {day_stores} p LEFT ANTI JOIN {S} s ON p.chain = s.chain AND p.store_id = s.store_id",
     0, None, "Stores publishing prices that are not in silver.stores; their prices are kept"),
    ("eilat_stores_missing", "business",
     f"SELECT count(*) FROM {S} s LEFT ANTI JOIN {day_stores} p ON p.chain = s.chain AND p.store_id = s.store_id WHERE s.is_eilat",
     0, None, "Eilat stores with no prices on the latest day"),
    ("price_jump_over_300pct", "business",
     f"SELECT count_if(n.price > o.price * 4) FROM (SELECT chain, store_id, item_code, price FROM {P} WHERE file_date = DATE'{latest}') n JOIN (SELECT chain, store_id, item_code, price FROM {P} WHERE file_date = DATE'{previous}') o ON n.chain = o.chain AND n.store_id = o.store_id AND n.item_code = o.item_code",
     100, None, "Products whose price more than quadrupled since the previous day"),
    ("history_days_mismatch", "row",
     f"SELECT abs((SELECT coalesce(sum(days_observed), 0) FROM {H}) - (SELECT count(*) FROM {P}))",
     None, 0, "Days covered by price_history differ from rows in price_obs"),
    ("history_overlapping_periods", "row",
     f"SELECT count_if(first_seen <= prev_last) FROM (SELECT first_seen, lag(last_seen) OVER (PARTITION BY chain, store_id, item_code ORDER BY first_seen) AS prev_last FROM {H})",
     None, 0, "Two periods of the same product at a store overlap"),
    ("items_repeated_keys", "row",
     f"SELECT count(*) FROM (SELECT 1 FROM {I} GROUP BY item_key HAVING count(*) > 1)",
     None, 0, "Same product key appears more than once in items"),
    ("price_obs_without_item", "row",
     f"SELECT count(*) FROM {P} p LEFT ANTI JOIN {I} i ON i.item_key = CASE WHEN p.item_type = '1' OR p.item_code RLIKE '^([0-9]{{8}}|[0-9]{{12,13}})$' THEN p.item_code ELSE concat(p.chain, ':', p.item_code) END WHERE p.file_date = DATE'{latest}'",
     None, 0, "Prices on the latest day whose product has no row in items"),
    ("items_without_name", "row",
     f"SELECT count_if(item_name IS NULL) FROM {I}",
     0, None, "Products in items with no name"),
    ("chain_without_price_files", "file",
     chains_without_files,
     None, 0, "Chains whose Stores file lists physical stores but that have no price rows on the latest date"),
    ("price_store_completeness", "file",
     completeness_gap,
     0, None, "Chains with more stores missing a price file on the latest date than on a normal recent day (beyond 2 stores or 3% of the chain)"),
    ("promotion_repeated_keys", "row",
     f"SELECT count(*) FROM (SELECT 1 FROM {PO} WHERE file_date = DATE'{promo_latest}' GROUP BY chain, store_id, promotion_id, group_id, item_code HAVING count(*) > 1)",
     None, 0, "Same chain, store, promotion, group, product and day appears more than once in promotion_obs"),
    ("promotion_reject_percent", "row",
     f"SELECT 100.0 * q.n / (q.n + p.n) FROM (SELECT count(*) AS n FROM {PQ} WHERE file_date = DATE'{promo_latest}') q, (SELECT count(*) AS n FROM {PO} WHERE file_date = DATE'{promo_latest}') p",
     0.1, 5, "Percent of the latest promotion day's rows that were rejected"),
    ("promotion_expired_percent", "business",
     f"SELECT 100.0 * count_if(end_at < file_date) / count(*) FROM {PO} WHERE file_date = DATE'{promo_latest}'",
     0.1, None, "Percent of the latest promotion day's rows already past their end date on the file date"),
]

# COMMAND ----------

def outcome_of(observed, warn_above, fail_above):
    if fail_above is not None and observed > fail_above:
        return "FAIL"
    if warn_above is not None and observed > warn_above:
        return "WARNING"
    return "PASS"

results = []
for name, level, sql, warn_above, fail_above, meaning in checks:
    observed = float(spark.sql(sql).first()[0])
    outcome = outcome_of(observed, warn_above, fail_above)
    print(outcome, name, observed)
    results.append((
        run_at, name, level, latest, observed,
        None if warn_above is None else float(warn_above),
        None if fail_above is None else float(fail_above),
        outcome, meaning,
    ))

schema = "run_at timestamp, check_name string, check_level string, file_date date, observed double, warn_above double, fail_above double, outcome string, meaning string"
spark.createDataFrame(results, schema).write.mode("append").saveAsTable("workspace.silver.dq_results")

# COMMAND ----------

failed = [r[1] for r in results if r[7] == "FAIL"]
if failed:
    raise Exception("Data quality checks failed: " + ", ".join(failed))
