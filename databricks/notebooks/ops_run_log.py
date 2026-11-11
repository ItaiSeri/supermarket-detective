# Databricks notebook source
# Final task of a job (run_if ALL_DONE). Writes one row per task run and target table
# into workspace.ops.run_log. Status and timing come from the Jobs API (no delay),
# row counts come from Delta table history (the `job` struct says which task run
# wrote each commit). Safe to run twice: MERGE on (task_run_id, target_table).
#
# Job parameters (set on the task in databricks.yml):
#   job_run_id = {{job.run_id}}     the run to describe
#   job_name   = {{job.name}}       only used in the note if the API has no name

# COMMAND ----------

# Pure helpers (no Spark, no network) so the logic can be tested from a laptop.

WRITE_OPS = (
    "STREAMING UPDATE", "MERGE", "WRITE", "DELETE", "UPDATE",
    "CREATE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT",
    "REPLACE TABLE AS SELECT",
)

STATUS = {
    "SUCCESS": "SUCCESS", "FAILED": "FAILED", "CANCELED": "CANCELED",
    "TIMEDOUT": "TIMEDOUT", "UPSTREAM_FAILED": "UPSTREAM_FAILED",
    "UPSTREAM_CANCELED": "UPSTREAM_CANCELED", "DISABLED": "SKIPPED",
    "EXCLUDED": "SKIPPED", "SUCCESS_WITH_FAILURES": "FAILED",
}


TASK_SCHEMA = (
    "job_id BIGINT, job_run_id BIGINT, job_name STRING, trigger_type STRING, task_key STRING, "
    "task_run_id BIGINT, attempt INT, status STRING, started_ms BIGINT, ended_ms BIGINT, note STRING"
)


def task_rows(run: dict, own_task_key: str) -> list:
    """One dict per finished task run of a job run (from jobs get-run, as_dict())."""
    out = []
    for t in run.get("tasks", []):
        if t["task_key"] == own_task_key:
            continue  # the logger does not log itself
        st = t.get("state", {})
        life = st.get("life_cycle_state")
        result = st.get("result_state")
        if life in ("PENDING", "RUNNING", "QUEUED", "BLOCKED", "WAITING_FOR_RETRY", "TERMINATING"):
            status = "RUNNING"
        elif life == "SKIPPED":
            status = STATUS.get(result, "SKIPPED")
        else:
            status = STATUS.get(result, result or life or "UNKNOWN")
        start = t.get("start_time") or 0
        end = t.get("end_time") or 0
        out.append({
            "job_id": int(run["job_id"]),
            "job_run_id": int(run["run_id"]),
            "job_name": run.get("run_name"),
            "trigger_type": run.get("trigger"),
            "task_key": t["task_key"],
            "task_run_id": int(t["run_id"]),
            "attempt": int(t.get("attempt_number", 0)),
            "status": status,
            "started_ms": start if start else None,
            "ended_ms": end if end else None,
            "note": (st.get("state_message") or "")[:500] or None,
        })
    return out


def history_sql(tables: list, job_run_id: int) -> str:
    """Rows written per (task run, table) by commits of this job run.
    DESCRIBE HISTORY cannot sit in a view, but it works as a subquery in a query."""
    parts = [
        f"SELECT '{t}' AS target_table, timestamp, operation, job.jobRunId AS job_run_id, "
        f"job.runId AS task_run_id, operationMetrics AS m FROM (DESCRIBE HISTORY workspace.{t})"
        for t in tables
    ]
    ops = ", ".join(f"'{o}'" for o in WRITE_OPS)
    return f"""
SELECT
  cast(task_run_id AS BIGINT) AS task_run_id,
  target_table,
  count(*) AS commits,
  sum(try_cast(m['numSourceRows'] AS BIGINT)) AS rows_in,
  sum(CASE WHEN operation = 'MERGE'
           THEN coalesce(try_cast(m['numTargetRowsInserted'] AS BIGINT), 0)
           ELSE coalesce(try_cast(m['numOutputRows'] AS BIGINT), 0) END) AS rows_inserted,
  sum(CASE WHEN operation = 'MERGE'
           THEN coalesce(try_cast(m['numTargetRowsUpdated'] AS BIGINT), 0)
           ELSE 0 END) AS rows_updated,
  sum(coalesce(try_cast(m['numTargetRowsDeleted'] AS BIGINT), 0)) AS rows_deleted
FROM ({' UNION ALL '.join(parts)})
WHERE cast(job_run_id AS BIGINT) = {int(job_run_id)}
  AND operation IN ({ops})
GROUP BY task_run_id, target_table
"""


# COMMAND ----------

# Table definition. Key = (task_run_id, target_table). target_table is '' when the
# task wrote nothing (failed early, skipped, or a load that found no new files).

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS workspace.ops.run_log (
  task_run_id    BIGINT    COMMENT 'Jobs API run id of the task run (history job.runId)',
  target_table   STRING    COMMENT 'schema.table written by the task; empty if nothing written',
  job_run_id     BIGINT    COMMENT 'Run id of the whole job run',
  job_id         BIGINT,
  job_name       STRING,
  task_key       STRING,
  attempt        INT       COMMENT 'Retry number, 0 = first try',
  trigger_type   STRING    COMMENT 'ONE_TIME (manual), PERIODIC, RUN_JOB_TASK ...',
  status         STRING    COMMENT 'SUCCESS, FAILED, CANCELED, TIMEDOUT, UPSTREAM_FAILED, SKIPPED, RUNNING',
  started_at     TIMESTAMP COMMENT 'UTC',
  ended_at       TIMESTAMP COMMENT 'UTC',
  duration_s     DOUBLE,
  commits        INT       COMMENT 'Delta commits this task made to the table',
  rows_in        BIGINT    COMMENT 'MERGE source rows; NULL for streaming and full rewrites',
  rows_inserted  BIGINT,
  rows_updated   BIGINT,
  rows_deleted   BIGINT,
  rows_written   BIGINT    COMMENT 'inserted + updated',
  note           STRING    COMMENT 'Error message of a failed task',
  logged_at      TIMESTAMP
)
"""

# COMMAND ----------

if __name__ == "__main__" or "spark" in globals():
    from databricks.sdk import WorkspaceClient

    OWN_TASK = "log_run"  # task_key of this notebook in databricks.yml

    dbutils.widgets.text("job_run_id", "")
    job_run_id = dbutils.widgets.get("job_run_id").strip()
    if not job_run_id:
        raise ValueError("Pass job_run_id = {{job.run_id}} as a task parameter")

    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")
    spark.sql(CREATE_SQL)

    run = WorkspaceClient().jobs.get_run(run_id=int(job_run_id)).as_dict()
    rows = task_rows(run, OWN_TASK)
    print(f"{run.get('run_name')}: {len(rows)} task runs")

    tables = [
        f"{r.table_schema}.{r.table_name}"
        for r in spark.sql("""
            SELECT table_schema, table_name FROM workspace.information_schema.tables
            WHERE table_schema IN ('bronze', 'silver') AND data_source_format = 'DELTA'
        """).collect()
    ]

    spark.createDataFrame(rows, schema=TASK_SCHEMA).createOrReplaceTempView("task_runs_in")
    spark.sql(history_sql(tables, int(job_run_id))).createOrReplaceTempView("history_in")

    spark.sql("""
    MERGE INTO workspace.ops.run_log AS t
    USING (
      SELECT
        r.task_run_id, coalesce(h.target_table, '') AS target_table,
        r.job_run_id, r.job_id, r.job_name, r.task_key, r.attempt, r.trigger_type, r.status,
        timestamp_millis(r.started_ms) AS started_at,
        timestamp_millis(r.ended_ms) AS ended_at,
        (r.ended_ms - r.started_ms) / 1000.0 AS duration_s,
        cast(h.commits AS INT) AS commits,
        h.rows_in, h.rows_inserted, h.rows_updated, h.rows_deleted,
        coalesce(h.rows_inserted, 0) + coalesce(h.rows_updated, 0) AS rows_written,
        r.note, current_timestamp() AS logged_at
      FROM task_runs_in r
      LEFT JOIN history_in h ON h.task_run_id = r.task_run_id
    ) AS s
    ON t.task_run_id = s.task_run_id AND t.target_table = s.target_table
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    display(spark.sql(f"""
      SELECT task_key, target_table, status, duration_s, rows_in, rows_written, note
      FROM workspace.ops.run_log WHERE job_run_id = {int(job_run_id)} ORDER BY started_at
    """))
