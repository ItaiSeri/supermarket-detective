# Databricks notebook source
# Helper SQL functions for silver. Run first in silver_load (task create_functions); safe to re-run.
#
# Repeated header names in Kaggle files (see bronze_kaggle.py): bronze keeps both values, the second one under
# `<name>_2` (third `_3`, ...). Silver resolves them once, on the whole map, before any field is read:
#   resolve_dup_keys(m)  : the map with X_2.. folded into X: the first non-empty value in key order wins
#                          (only X filled -> X, only X_2 filled -> X_2, both equal -> that value)
#   has_dup_conflict(m)  : true when some X and X_2.. are both filled with DIFFERENT values; such a row goes to
#                          quarantine with reject_reason 'duplicate_column_conflict' and is not loaded
# Maps without a key ending in _2.._9 are returned untouched, and cost one cheap check per row.

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# COMMAND ----------

spark.sql(r"""
CREATE OR REPLACE FUNCTION workspace.silver.resolve_dup_keys(m MAP<STRING, STRING>)
RETURNS MAP<STRING, STRING>
RETURN CASE
  WHEN m IS NULL OR NOT exists(map_keys(m), k -> k RLIKE '_[2-9]$') THEN m
  ELSE map_from_entries(
    transform(
      array_distinct(transform(map_keys(m), k -> regexp_replace(k, '_[2-9]$', ''))),
      b -> struct(
        b,
        coalesce(
          try_element_at(
            filter(
              transform(array_sort(filter(map_keys(m), k -> regexp_replace(k, '_[2-9]$', '') = b)), k -> m[k]),
              v -> trim(v) <> ''),
            1),
          m[b]))))
END
""")

spark.sql(r"""
CREATE OR REPLACE FUNCTION workspace.silver.has_dup_conflict(m MAP<STRING, STRING>)
RETURNS BOOLEAN
RETURN coalesce(
  CASE
    WHEN m IS NULL OR NOT exists(map_keys(m), k -> k RLIKE '_[2-9]$') THEN false
    ELSE exists(
      array_distinct(transform(map_keys(m), k -> regexp_replace(k, '_[2-9]$', ''))),
      b -> size(array_distinct(
             filter(
               transform(filter(map_keys(m), k -> regexp_replace(k, '_[2-9]$', '') = b), k -> trim(m[k])),
               v -> v <> ''))) > 1)
  END,
  false)
""")

# COMMAND ----------

# Self-test: the cases of the rule
test = spark.sql("""
SELECT
  workspace.silver.resolve_dup_keys(map('MaxQty', '3', 'MaxQty_2', NULL, 'A', 'x'))['MaxQty'] AS only_first,
  workspace.silver.resolve_dup_keys(map('MaxQty_2', '5', 'A', 'x'))['MaxQty'] AS only_second,
  workspace.silver.resolve_dup_keys(map('MaxQty', '4', 'MaxQty_2', '4'))['MaxQty'] AS equal,
  workspace.silver.resolve_dup_keys(map('MaxQty', '', 'MaxQty_2', '6'))['MaxQty'] AS empty_first,
  workspace.silver.has_dup_conflict(map('MaxQty', '4', 'MaxQty_2', '5')) AS conflict,
  workspace.silver.has_dup_conflict(map('MaxQty', '4', 'MaxQty_2', '4')) AS no_conflict_equal,
  workspace.silver.has_dup_conflict(map('MaxQty', '4', 'B_2', '4')) AS no_conflict_other_key,
  workspace.silver.has_dup_conflict(CAST(NULL AS MAP<STRING, STRING>)) AS null_map,
  size(workspace.silver.resolve_dup_keys(map('A', '1', 'B', '2'))) AS untouched
""").first()
print(test)
assert (test.only_first, test.only_second, test.equal, test.empty_first) == ("3", "5", "4", "6")
assert test.conflict and not test.no_conflict_equal and not test.no_conflict_other_key and not test.null_map
assert test.untouched == 2
