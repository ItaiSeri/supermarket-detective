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

# COMMAND ----------

# Store ID from the source file name (decided 2026-11-17): the file name is the chain's own address of the store and
# matches the chain's Stores file; the in-file header can carry an OLD store code (Carrefour: 78 of 152 files on
# 2026-11-17, 19 old codes equal another real store's ID, e.g. 420 = Carrefour Market Eilat).
#   <PriceFull|PromoFull|Price|Promo><13-digit chain id>-[<sub-chain>-]<store>-<yyyymmdd>-<hhmmss | yyyymmddhhmm>
# The store is the last number before the date. Leading zeros are dropped like everywhere else. NULL when the
# name does not fit (the notebooks then fall back to the header and mark store_id_source = 'header').
spark.sql(r"""
CREATE OR REPLACE FUNCTION workspace.silver.store_id_from_file_name(file_name STRING)
RETURNS STRING
COMMENT 'Store ID (no leading zeros) from a chain file name, NULL when the name has no store part'
RETURN nullif(regexp_replace(
  regexp_extract(file_name, '(?i)^[a-z]+[0-9]{13}-(?:[0-9]+-)*?([0-9]+)-20[0-9]{6}-?[0-9]{4,6}', 1),
  '^0+(?=.)', ''), '')
""")

sid_test = spark.sql("""
SELECT
  workspace.silver.store_id_from_file_name('PriceFull7290055700007-001-400-20261117-051002.xml.gz') AS carrefour,
  workspace.silver.store_id_from_file_name('PriceFull7290803800003-7999-202405221225.xml.gz')       AS no_subchain,
  workspace.silver.store_id_from_file_name('pricefull7290058140886-039-202611170600.xml.gz')        AS rami_levy,
  workspace.silver.store_id_from_file_name('PromoFull7290803800003-00-1-202611170600.xml.gz')       AS yohananof_promo,
  workspace.silver.store_id_from_file_name('PriceFull7290055700007-001-008-20261117-051002.gz.xml.gz') AS leading_zeros,
  workspace.silver.store_id_from_file_name('Stores7290055700007-000-20261117-051002.xml.gz')        AS stores_file
""").first()
print(sid_test)
assert (sid_test.carrefour, sid_test.no_subchain, sid_test.rami_levy, sid_test.yohananof_promo, sid_test.leading_zeros) ==        ("400", "7999", "39", "1", "8")

# COMMAND ----------

# Chain ID aliases: a chain whose files carried another ChainID in the past. Silver's wrong_chain check
# (price_obs, promotion_obs, items) accepts a row when its header ChainID equals the chain's own ID OR an alias row
# matches the chain, the ID and the file date. Rows keep the original chain_id value (provenance).
# CREATE OR REPLACE + INSERT, so re-running gives the same table.
spark.sql("""
CREATE OR REPLACE TABLE workspace.silver.chain_id_aliases (
  chain STRING, chain_id STRING, valid_from DATE, valid_to DATE, reason STRING)
""")
spark.sql("""
INSERT INTO workspace.silver.chain_id_aliases VALUES (
  'YAYNO_BITAN_AND_CARREFOUR', '7290725900003', DATE'2024-01-01', DATE'2025-07-13',
  'Legacy Yayno Bitan chain ID in Kaggle files before the Carrefour feed; 19 shared store numbers have identical address+ZIP, 9 others unused by the new ID; checked 2026-11-16')
""")

# COMMAND ----------

# Product key rule (S5), defined once and used by silver_items (scope) and gold (fact keys).
# A code is 'shared' (the same product in every chain) when ItemType is 1 or it has the shape of a barcode
# (8, 12 or 13 digits). Anything else is the chain's own code, so its key adds the chain: CHAIN:code.
spark.sql("""
CREATE OR REPLACE FUNCTION workspace.silver.item_scope(item_code STRING, item_type STRING)
RETURNS STRING
COMMENT 'shared when ItemType is 1 or the code is 8/12/13 digits, else chain'
RETURN CASE WHEN item_type = '1' OR item_code RLIKE '^([0-9]{8}|[0-9]{12,13})$' THEN 'shared' ELSE 'chain' END
""")
spark.sql("""
CREATE OR REPLACE FUNCTION workspace.silver.item_key(chain STRING, item_code STRING, item_type STRING)
RETURNS STRING
COMMENT 'Product key: the code itself when shared across chains, else CHAIN:code'
RETURN CASE WHEN workspace.silver.item_scope(item_code, item_type) = 'shared' THEN item_code
            ELSE concat(chain, ':', item_code) END
""")

key_test = spark.sql("""
SELECT
  workspace.silver.item_key('KESHET', '6438', '0')          AS chain_only,
  workspace.silver.item_key('KESHET', '7290107280679', '0') AS barcode_13,
  workspace.silver.item_key('KESHET', '12345', '1')         AS item_type_1,
  workspace.silver.item_scope('7290107280679', '0')         AS scope_barcode
""").first()
print(key_test)
assert (key_test.chain_only, key_test.barcode_13, key_test.item_type_1, key_test.scope_barcode) == \
       ("KESHET:6438", "7290107280679", "12345", "shared")

# COMMAND ----------

# GS1 barcode flag (decided 2026-11-19). item_key and item_scope are deliberately NOT changed by it. Cross-chain
# comparisons and baskets use only items where is_gs1_barcode is true: in-store codes 7290000000xxx are reused by
# different chains for different products, and in-store codes (mostly weighed meat and fish) fail the check digit.
# True when the code is 8, 12 or 13 digits, does not start with 7290000000, and its GS1 mod-10 check digit is valid
# (EAN-13, EAN-8, UPC-A 12 digits: from the right, skipping the check digit, weights 3,1,3,1,...).
spark.sql(r"""
CREATE OR REPLACE FUNCTION workspace.silver.is_gs1_barcode(item_code STRING)
RETURNS BOOLEAN
COMMENT 'True for an 8/12/13-digit code not starting with 7290000000 whose GS1 mod-10 check digit is valid'
RETURN coalesce(
  item_code RLIKE '^([0-9]{8}|[0-9]{12,13})$'
  AND NOT item_code LIKE '7290000000%'
  AND (10 - (aggregate(
        transform(sequence(0, length(item_code) - 2),
                  i -> CAST(substr(reverse(item_code), i + 2, 1) AS INT) * IF(i % 2 = 0, 3, 1)),
        0, (a, x) -> a + x) % 10)) % 10 = CAST(substr(item_code, -1) AS INT),
  false)
""")

gs1_test = spark.sql("""
SELECT
  workspace.silver.is_gs1_barcode('7290107280679') AS real_13,
  workspace.silver.is_gs1_barcode('80176800')      AS real_8_nutella,
  workspace.silver.is_gs1_barcode('7290000000249') AS in_store_prefix,
  workspace.silver.is_gs1_barcode('7290000360741') AS bad_check_digit,
  workspace.silver.is_gs1_barcode('78013007')      AS bad_check_8,
  workspace.silver.is_gs1_barcode('374')           AS short_code,
  workspace.silver.is_gs1_barcode(NULL)            AS null_code
""").first()
print(gs1_test)
assert gs1_test.real_13 and gs1_test.real_8_nutella
assert not (gs1_test.in_store_prefix or gs1_test.bad_check_digit or gs1_test.bad_check_8 or gs1_test.short_code or gs1_test.null_code)
