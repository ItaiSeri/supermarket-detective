"""One-off: fill workspace.ops.backfill_queue from ingestion/fetch_kaggle/backfill_versions.json.

Run from the laptop (it only prints SQL; the README shows how to send it to Databricks):
  python databricks/seed_backfill_queue.py > seed.sql
It inserts only versions that are not in the table yet, so a second run is harmless. Status starts as 'pending'
even for versions whose files are already in silver: a reload is safe (silver MERGEs are idempotent).
The 5 extra versions of backfill_versions_extra.json were added by hand the same way (status pending), and the
versions they replace (446, 271, 93, 89, 83) were set to status skipped.
The table itself is created by notebook backfill_queue.py (task claim_batch), or by hand with the same DDL.
"""
import json
import pathlib

path = pathlib.Path(__file__).parent.parent / "ingestion/fetch_kaggle/backfill_versions.json"
versions = json.loads(path.read_text(encoding="utf-8"))["versions"]
values = ",\n  ".join(
    f"({v['version']}, DATE'{v['version_date']}', {v['load_order']}, '{','.join(v['kinds'])}')" for v in versions
)
print(f"""INSERT INTO workspace.ops.backfill_queue (version, version_date, load_order, kinds, status, attempts)
SELECT s.version, s.version_date, s.load_order, s.kinds, 'pending', 0
FROM (VALUES
  {values}
) AS s(version, version_date, load_order, kinds)
WHERE s.version NOT IN (SELECT version FROM workspace.ops.backfill_queue)""")
