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

latest = spark.sql(f"SELECT max(file_date) AS d FROM {P}").first()["d"]
previous = spark.sql(f"SELECT max(file_date) AS d FROM {P} WHERE file_date < DATE'{latest}'").first()["d"]
collected = spark.sql(f"SELECT max(collected_date) AS d FROM {P}").first()["d"]
run_at = spark.sql("SELECT current_timestamp() AS t").first()["t"]

day_stores = f"(SELECT DISTINCT chain, store_id FROM {P} WHERE file_date = DATE'{latest}')"

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
