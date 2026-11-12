# Databricks notebook source
# Queue steps of job kaggle_backfill (one run = one batch of Kaggle versions). Table: workspace.ops.backfill_queue,
# one row per Kaggle version, seeded by databricks/seed_backfill_queue.py from ingestion/fetch_kaggle/backfill_versions.json.
#
#   mode = claim   first task. Picks the batch and marks it `running`:
#                    1. a batch left `running` or `failed` by an earlier run (fewer than MAX_ATTEMPTS attempts), else
#                    2. the next `batch_size` `pending` versions in load_order.
#                  Sets task value go = yes | no. `no` when there is nothing to do, or when daily_load is running
#                  (the Free Edition allows 5 concurrent tasks, and the morning load comes first).
#   mode = finish  after the loads and silver (run_if ALL_DONE). Reads this run's task states from the Jobs API:
#                  all succeeded -> `done` with the rows each version added to bronze; else `failed` with the error.
#
# A batch that fails MAX_ATTEMPTS times is left `failed` and skipped, so one bad version does not block the
# queue and does not burn the daily quota in a loop. To retry it: UPDATE ... SET attempts = 0.

dbutils.widgets.text("mode", "claim")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("batch_size", "5")
dbutils.widgets.text("daily_load_job_id", "")
MODE = dbutils.widgets.get("mode")
RUN_ID = dbutils.widgets.get("job_run_id").strip()
BATCH_SIZE = int(dbutils.widgets.get("batch_size") or 5)
DAILY_LOAD_JOB_ID = dbutils.widgets.get("daily_load_job_id").strip()
MAX_ATTEMPTS = 3
Q = "workspace.ops.backfill_queue"

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {Q} (
  version      INT,        -- Kaggle dataset version number
  version_date DATE,       -- its upload date = the date= folder in S3
  load_order   INT,        -- 1 = loaded first (Hanukkah 2025 window, Hanukkah 2024 window, then weekly newest to oldest)
  kinds        STRING,     -- comma list of file kinds the version is loaded with
  status       STRING,     -- pending | running | done | failed
  attempts     INT,
  batch_id     STRING,     -- the job run that claimed it
  started_at   TIMESTAMP,
  ended_at     TIMESTAMP,
  job_run_id   STRING,
  rows_price   BIGINT,
  rows_promo   BIGINT,
  rows_stores  BIGINT,
  error        STRING
) USING DELTA
""")

# COMMAND ----------

def in_clause(versions):
    return ", ".join(str(int(v)) for v in versions)


def claim():
    if DAILY_LOAD_JOB_ID:
        from databricks.sdk import WorkspaceClient
        active = list(WorkspaceClient().jobs.list_runs(job_id=int(DAILY_LOAD_JOB_ID), active_only=True))
        if active:
            print("daily_load is running; this run does nothing")
            return []
    left = spark.sql(f"""SELECT batch_id FROM {Q} WHERE status IN ('running', 'failed') AND attempts < {MAX_ATTEMPTS}
                         ORDER BY load_order LIMIT 1""").collect()
    if left:
        rows = spark.sql(f"""SELECT version FROM {Q} WHERE batch_id = '{left[0].batch_id}' AND status IN ('running', 'failed')
                             AND attempts < {MAX_ATTEMPTS}""").collect()
        print(f"re-claiming the batch {left[0].batch_id}")
    else:
        rows = spark.sql(f"SELECT version FROM {Q} WHERE status = 'pending' ORDER BY load_order LIMIT {BATCH_SIZE}").collect()
    versions = [r.version for r in rows]
    if versions:
        spark.sql(f"""UPDATE {Q} SET status = 'running', batch_id = '{RUN_ID}', job_run_id = '{RUN_ID}',
                      started_at = current_timestamp(), ended_at = NULL, attempts = attempts + 1, error = NULL
                      WHERE version IN ({in_clause(versions)})""")
    return versions


def finish():
    from databricks.sdk import WorkspaceClient
    rows = spark.sql(f"SELECT version, version_date FROM {Q} WHERE status = 'running' AND job_run_id = '{RUN_ID}'").collect()
    if not rows:
        print("no batch in this run")
        return
    run = WorkspaceClient().jobs.get_run(int(RUN_ID))
    bad = []
    for t in run.tasks or []:
        if t.task_key in ("load_kaggle_prices", "load_kaggle_stores", "load_kaggle_promotions", "run_silver"):
            result = t.state.result_state.value if t.state and t.state.result_state else str(t.state.life_cycle_state)
            if result != "SUCCESS":
                bad.append(f"{t.task_key}: {result} {t.state.state_message or ''}".strip())
    versions = [r.version for r in rows]
    if bad:
        error = ("; ".join(bad))[:1000].replace("'", " ")
        spark.sql(f"UPDATE {Q} SET status = 'failed', ended_at = current_timestamp(), error = '{error}' WHERE version IN ({in_clause(versions)})")
        print("batch failed:", error)
        return
    dates = ", ".join(f"DATE'{r.version_date}'" for r in rows)
    spark.sql(f"""
    MERGE INTO {Q} q USING (
      SELECT d.version_date,
             coalesce(p.n, 0) AS rows_price, coalesce(m.n, 0) AS rows_promo, coalesce(s.n, 0) AS rows_stores
      FROM (SELECT DISTINCT version_date FROM {Q} WHERE version IN ({in_clause(versions)})) d
      LEFT JOIN (SELECT collected_date, count(*) n FROM workspace.bronze.price_items WHERE source = 'kaggle' AND collected_date IN ({dates}) GROUP BY 1) p ON p.collected_date = d.version_date
      LEFT JOIN (SELECT collected_date, count(*) n FROM workspace.bronze.promotion_items WHERE source = 'kaggle' AND collected_date IN ({dates}) GROUP BY 1) m ON m.collected_date = d.version_date
      LEFT JOIN (SELECT collected_date, count(*) n FROM workspace.bronze.stores WHERE source = 'kaggle' AND collected_date IN ({dates}) GROUP BY 1) s ON s.collected_date = d.version_date
    ) c ON q.version_date = c.version_date AND q.version IN ({in_clause(versions)})
    WHEN MATCHED THEN UPDATE SET q.status = 'done', q.ended_at = current_timestamp(), q.error = NULL,
      q.rows_price = c.rows_price, q.rows_promo = c.rows_promo, q.rows_stores = c.rows_stores""")
    print(f"batch done: {len(versions)} version(s)")

# COMMAND ----------

if MODE == "claim":
    versions = claim()
    print("claimed versions:", versions)
    dbutils.jobs.taskValues.set("go", "yes" if versions else "no")
elif MODE == "finish":
    finish()
else:
    raise ValueError(MODE)
