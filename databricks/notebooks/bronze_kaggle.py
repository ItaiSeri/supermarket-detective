# Databricks notebook source
# Loads the Kaggle backfill files (raw: s3://.../source=kaggle/date=*/chain=*/*.csv.gz) into the
# SAME bronze tables as the scraper, with source = 'kaggle', in the same row shape (one row per
# product / store / promoted product, maps of values as published).
#
# One task per file kind, set by the parameter `kind`:  price | store | promo
#
# Kaggle's CSV is sparse: a value equal to the value on the row above is left blank, in every
# column. A block of rows is one source file and starts at a row with a non-empty file_name. So:
#   1. read every row as text (header=false: files of different versions have different headers)
#   2. take each file's header from its first row and name every cell by it
#   3. fill each blank from the row above, inside the block only, in the original row order
#   4. the literal '' means "empty in the source" and becomes NULL (it is never filled)
#   5. rename columns to the scraper's field names and build the header / fields maps
# Row order survives because a .gz file can't be split: one file is read by one task, in order.
# Line ends are mixed in these files (CRLF on the header and some rows, LF on the rest), so the reader is
# told lineSep = LF; otherwise Spark takes the first line's CRLF as the only separator and merges rows.
# The stray CR then sits at the end of the last cell of a CRLF row and is trimmed.
# Repeated header names: a header may spell one tag twice with different capitalisation (Rami Levy promotions:
# MaxQty and MAXQTY; Hazi Hinam prices: BisWeighted and bIsWeighted), because different stores' XML spell it
# differently. After lowercasing they would collide. The 2nd occurrence is renamed `<name>_2` (3rd `_3`...), in
# header order, and carried into the bronze maps under `<ScraperName>_2`. Nothing is dropped or merged here;
# silver (resolve_dup_keys in silver_functions) picks the filled one, or quarantines a conflict.
# Safety net: if a chunk fails to write, its files are retried one by one; a file that fails alone becomes
# ONE bronze row with parse_error set (like the scraper's bronze), and the load goes on.
# Queue mode (since 2026-11-12): the files to load are NOT found by listing the whole bucket. The job takes the
# versions of the batch that task `claim_batch` marked `running` in workspace.ops.backfill_queue, deletes any
# bronze rows of those versions' date= folders (so a retry after a crash never doubles rows), and loads exactly
# those folders' files of this kind. Raw lives in the Ohio bucket next to Databricks; bronze is a managed table.

import csv
import re

dbutils.widgets.text("kind", "price")
dbutils.widgets.text("path_contains", "")  # test only: load just the files whose path contains this text
dbutils.widgets.text("files_per_write", "30")
dbutils.widgets.text("job_run_id", "")  # the batch to load: queue rows `running` with this job_run_id; empty = any running batch
dbutils.widgets.text("kaggle_root", "s3://supermarket-detective-kaggle-971422709739/source=kaggle")
KIND = dbutils.widgets.get("kind")
PATH_CONTAINS = dbutils.widgets.get("path_contains").strip()
FILES_PER_WRITE = int(dbutils.widgets.get("files_per_write"))
JOB_RUN_ID = dbutils.widgets.get("job_run_id").strip()
KAGGLE_ROOT = dbutils.widgets.get("kaggle_root").rstrip("/")
QUEUE = "workspace.ops.backfill_queue"

# A promotion that lists MORE than this many products in one file is "store-wide" (see bronze_promotion_items.py):
# one bronze row with item NULL, store_wide = true. The count is taken before any item is built.
STORE_WIDE_MIN_ITEMS = 10000

N_COLS = 64  # the widest header (promotions) has 40 columns

KINDS = {
    "price": {"pattern": "price_full_file_*.csv.gz", "table": "workspace.bronze.price_items", "queue_kind": "price_full_file"},
    "store": {"pattern": "store_file_*.csv.gz", "table": "workspace.bronze.stores", "queue_kind": "store_file"},
    "promo": {"pattern": "promo_full_file_*.csv.gz", "table": "workspace.bronze.promotion_items", "queue_kind": "promo_full_file"},
}
CFG = KINDS[KIND]

# COMMAND ----------

# Kaggle column (lowercase) -> the field name the scraper's bronze rows use. Silver reads these names.

PRICE_HEADER = {"chainid": "ChainID", "subchainid": "SubChainID", "storeid": "StoreID", "bikoretno": "BikoretNo"}
PRICE_FIELDS = {
    "priceupdatedate": "PriceUpdateDate", "priceupdatetime": "PriceUpdateTime",
    "lastupdatedate": "LastUpdateDate", "lastupdatetime": "LastUpdateTime",
    "itemcode": "ItemCode", "itemtype": "ItemType", "itemname": "ItemName", "itemnm": "ItemNm", "itemid": "ItemId",
    "manufacturername": "ManufacturerName", "manufacturename": "ManufactureName",
    "manufacturecountry": "ManufactureCountry",
    "manufactureritemdescription": "ManufacturerItemDescription",
    "manufactureitemdescription": "ManufactureItemDescription",
    "unitqty": "UnitQty", "quantity": "Quantity", "bisweighted": "bIsWeighted",
    "unitofmeasure": "UnitOfMeasure", "unitmeasure": "UnitOfMeasure",  # one older feed spells it unitmeasure
    "qtyinpackage": "QtyInPackage", "itemprice": "ItemPrice", "unitofmeasureprice": "UnitOfMeasurePrice",
    "allowdiscount": "AllowDiscount", "itemstatus": "ItemStatus", "lastsaledatetime": "LastSaleDateTime",
}
STORE_HEADER = {
    "chainid": "ChainID", "chainname": "ChainName", "lastupdatedate": "LastUpdateDate",
    "lastupdatetime": "LastUpdateTime", "subchainid": "SubChainID", "subchainname": "SubChainName",
}
STORE_FIELDS = {
    "storeid": "StoreID", "bikoretno": "BikoretNo", "storetype": "StoreType", "storename": "StoreName",
    "address": "Address", "city": "City", "zipcode": "ZIPCode", "latitude": "Latitude", "longitude": "Longitude",
}
PROMO_HEADER = PRICE_HEADER

# Promotions. Kaggle flattened the two XML layouts into one set of columns. A row is one promotion.
#   groups layout : the `groups` column holds the items (JSON); the promotion keys are spelled like the
#                   scraper's Groups layout (PromotionID, ClubID...). Silver recognises the layout by
#                   the spelling `PromotionId` (flat) versus `PromotionID` (groups), so it matters.
#   flat layout   : the `promotionitems` column holds the items (JSON), or (one older feed, Victory and
#                   Mahsani Ashuk) every row is one promotion + one product in the columns itemcode/itemtype.
# Each entry is (target key, SQL expression over the Kaggle column names).
PROMO_COMMON = [
    ("PromotionDescription", "promotiondescription"),
    ("PromotionStartHour", "promotionstarthour"), ("PromotionEndHour", "promotionendhour"),
    ("PromotionStartDateTime", "promotionstartdatetime"), ("PromotionEndDateTime", "promotionenddatetime"),
    ("PromotionStartDate", "promotionstartdate"), ("PromotionEndDate", "promotionenddate"),
    ("PromotionUpdateTime", "promotionupdatetime"), ("PromotionUpdateDate", "promotionupdatedate"),
    ("AllowMultipleDiscounts", "allowmultiplediscounts"),
    ("AdditionalIsCoupon", "coalesce(av_coupon, additionaliscoupon, additionalscoupon)"),
    ("PriceUpdateDate", "priceupdatedate"),
]
PROMO_GROUPS_ONLY = [
    ("PromotionID", "promotionid"), ("PromotionDays", "promotiondays"), ("RedemptionLimit", "redemptionlimit"),
    ("MinNoOfItemOffered", "coalesce(minnoofitemoffered, minnoofitemofered, minnoofitemsoffered)"),
    ("ClubID", "coalesce(clubid, club_ids)"), ("IsGiftItem", "isgiftitem"),
    ("Remarks", "coalesce(remarks, remark)"),
]
PROMO_FLAT_ONLY = [
    ("PromotionId", "promotionid"), ("RewardType", "rewardtype"), ("IsWeightedPromo", "isweightedpromo"),
    ("MinQty", "minqty"), ("MaxQty", "maxqty"), ("DiscountedPrice", "discountedprice"),
    ("DiscountedPricePerMida", "discountedpricepermida"), ("DiscountRate", "discountrate"),
    ("DiscountType", "discounttype"), ("MinAmount", "minamount"), ("MaxAmount", "maxamount"),
    ("MinPurchaseAmnt", "coalesce(minpurchaseamnt, minpurchaseamount)"),
    ("MinNoOfItemOfered", "coalesce(minnoofitemofered, minnoofitemoffered, minnoofitemsoffered)"),
    ("WeightUnit", "weightunit"), ("Remark", "remark"), ("Remarks", "remarks"),
    ("ClubId", "coalesce(club_ids, clubid)"),
    ("AdditionalGiftCount", "coalesce(av_gift, additionalsgiftcount)"),
    ("AdditionalIsTotal", "coalesce(av_total, additionalstotals)"),
    ("AdditionalIsActive", "av_active"),
    ("AdditionalMinBasketAmount", "additionalsminbasketamount"),
]
# product fields in the JSON of the items (lowercase -> scraper key)
PROMO_ITEM_NAMES = {
    "itemcode": "ItemCode", "itemtype": "ItemType", "isgiftitem": "IsGiftItem", "groupid": "GroupID",
    "minpurchaseamount": "MinPurchaseAmount", "discounttype": "DiscountType", "rewardtype": "RewardType",
    "minqty": "MinQty", "maxqty": "MaxQty", "discountrate": "DiscountRate", "discountedprice": "DiscountedPrice",
    "discountedpricepermida": "DiscountedPricePerMida", "bisweighted": "bIsWeighted",
}
PROMO_KNOWN = {
    "chainid", "subchainid", "storeid", "bikoretno", "file_name", "groups", "promotionitems", "clubs", "clubid",
    "additionalrestrictions", "additionaliscoupon", "additionalscoupon", "additionalsgiftcount", "additionalstotals",
    "additionalsminbasketamount", "itemcode", "itemtype", "isgiftitem", "priceupdatedate",
    "promotionid", "minnoofitemofered", "minnoofitemoffered", "minnoofitemsoffered", "minpurchaseamnt",
    "minpurchaseamount", "remark", "remarks", "maxamount", "minamount", "weightunit", "promotiondays",
    "redemptionlimit", "rewardtype", "isweightedpromo", "minqty", "maxqty", "discountedprice",
    "discountedpricepermida", "discountrate", "discounttype", "promotiondescription", "promotionstarthour",
    "promotionendhour", "promotionstartdatetime", "promotionenddatetime", "promotionstartdate",
    "promotionenddate", "promotionupdatetime", "promotionupdatedate", "allowmultiplediscounts",
}

# COMMAND ----------

# Pure SQL builders (no Spark calls), so they can be tested without a cluster.

def q(name):
    return f"`{name}`"


def map_sql(pairs):
    """map('Key', expr, ...) from (key, expression) pairs, dropping the entries whose value is NULL."""
    entries = ", ".join(f"'{k}', {e}" for k, e in pairs)
    return f"map_filter(map({entries}), (k, v) -> v IS NOT NULL)"


def rename_sql(mapping):
    """SQL expression: a map<string,string> literal lowercase name -> scraper name."""
    return "map(" + ", ".join(f"'{k}', '{v}'" for k, v in mapping.items()) + ")"


def dedup_names(names):
    """Header names as the SQL sees them: lowercase, CR trimmed, a repeated name gets _2, _3... in header order."""
    seen, out = {}, []
    for n in names:
        low = n.rstrip("\r").lower()
        seen[low] = seen.get(low, 0) + 1
        out.append(low if seen[low] == 1 else f"{low}_{seen[low]}")
    return out


def dup_suffix(name):
    """('maxqty', 2) for 'maxqty_2'; None when the name has no _2.._9 suffix."""
    m = re.match(r"^(.+)_([2-9])$", name)
    return (m.group(1), int(m.group(2))) if m else None


def dup_pairs(pairs, present):
    """For every (target, expression) whose columns have a repeated twin (`col_2`...) in `present`, one more pair
    (target_2, same expression over the twins). Only the twins that exist are used."""
    out = []
    for target, expr in pairs:
        idents = [i for i in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr) if i != "coalesce"]
        for n in range(2, 10):
            twins = [f"{i}_{n}" for i in idents if f"{i}_{n}" in present]
            if twins:
                out.append((f"{target}_{n}", q(twins[0]) if len(twins) == 1 else f"coalesce({', '.join(q(t) for t in twins)})"))
    return out


def with_dups(pairs, present):
    return pairs + dup_pairs(pairs, present)


def plain_pairs(mapping, present):
    """(target key, expression) for the columns present. Two Kaggle spellings of one key are coalesced:
    a map with a repeated key is an error in Spark."""
    by_target = {}
    for src, target in mapping.items():
        if src in present:
            by_target.setdefault(target, []).append(q(src))
    return [(t, cols[0] if len(cols) == 1 else f"coalesce({', '.join(cols)})") for t, cols in by_target.items()]


def fill_sql(keys, raw_view="raw0"):
    """Steps 2-4: name the cells, fill blanks inside each block, ''->NULL. Returns the CTE chain; final CTE is `clean`."""
    cells = ", ".join(f"_c{i}" for i in range(N_COLS))
    fill_keys = [k for k in keys if k != "file_name"]
    cols = ",\n    ".join(f"nullif(m['{k}'], '') AS {q(k)}" for k in keys)
    fills = ",\n    ".join(
        f"last(CASE WHEN {q(k)} IS NOT NULL THEN struct(mid AS m, {q(k)} AS v) END, true) OVER w AS {q('f_' + k)}"
        for k in fill_keys
    )
    values = ",\n    ".join(
        f"nullif(CASE WHEN {q('f_' + k)}.m >= blk THEN {q('f_' + k)}.v END, \"''\") AS {q(k)}" for k in fill_keys
    )
    return f"""
numbered AS (
  SELECT file_path, modified_at, mid, array({cells}) AS cells,
         first(array({cells})) OVER (PARTITION BY file_path ORDER BY mid) AS hdr_cells,
         row_number() OVER (PARTITION BY file_path ORDER BY mid) AS rn
  FROM {raw_view}
),
named AS (
  -- the header names, lowercase; a name that repeats (case-insensitively) gets _2, _3... in header order
  SELECT file_path, modified_at, mid, cells,
         transform(hn0, (x, i) -> CASE WHEN size(filter(slice(hn0, 1, i), y -> y = x)) = 0 THEN x
                                       ELSE concat(x, '_', size(filter(slice(hn0, 1, i), y -> y = x)) + 1) END) AS hn
  FROM (SELECT *, transform(filter(hdr_cells, x -> x IS NOT NULL), x -> lower(rtrim(char(13), x))) AS hn0
        FROM numbered
        WHERE rn > 1)
),
mapped AS (
  SELECT file_path, modified_at, mid,
         map_from_arrays(
           hn,
           transform(slice(cells, 1, size(hn)),
                     (x, i) -> CASE WHEN i = size(hn) - 1 THEN rtrim(char(13), x) ELSE x END)) AS m
  FROM named
),
cols AS (
  SELECT file_path, modified_at, mid,
    {cols}
  FROM mapped
),
filled AS (
  SELECT file_path, modified_at, mid,
    last(CASE WHEN file_name IS NOT NULL THEN mid END, true) OVER w AS blk,
    last(file_name, true) OVER w AS src_file,
    {fills}
  FROM cols
  WINDOW w AS (PARTITION BY file_path ORDER BY mid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
clean AS (
  SELECT file_path, modified_at, mid, blk, src_file,
    {values}
  FROM filled
)"""


COMMON_SELECT = """
  file_path,
  src_file AS file_name,
  'kaggle' AS source,
  regexp_extract(file_path, 'chain=([^/]+)', 1) AS chain,
  to_date(regexp_extract(file_path, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1)) AS collected_date,
  modified_at"""


def price_or_store_sql(kind, keys, raw_view="raw0"):
    header_map, field_map = (PRICE_HEADER, PRICE_FIELDS) if kind == "price" else (STORE_HEADER, STORE_FIELDS)
    present = set(keys)
    known = set(header_map) | set(field_map) | {"file_name"}
    # a column we have no scraper name for is kept under its own (lowercase) name rather than dropped
    # (a repeated twin `col_2` of a known column is carried through with_dups instead)
    extra = [(k, q(k)) for k in keys if k not in known and not (dup_suffix(k) and dup_suffix(k)[0] in known)]
    header = map_sql(with_dups(plain_pairs(header_map, present), present))
    fields = map_sql(with_dups(plain_pairs(field_map, present), present) + extra)
    if kind == "price":
        shape = f"""CAST(NULL AS STRING) AS declared_count,
  CAST(NULL AS INT) AS tags_in_file,
  CAST(NULL AS STRING) AS parse_error,
  {header} AS header,
  {fields} AS fields,
  current_timestamp() AS loaded_at"""
    else:
        shape = f"""CAST(NULL AS INT) AS tags_in_file,
  CAST(NULL AS STRING) AS parse_error,
  {header} AS header,
  {fields} AS fields,
  current_timestamp() AS loaded_at"""
    return f"WITH {fill_sql(keys, raw_view).lstrip()}\nSELECT {COMMON_SELECT},\n  {shape}\nFROM clean"


def vget(variant, name):
    """The value of key `name` in a JSON object variant, whatever the key's capitalisation (Dec 2024 files:
    Item, ItemCode, ClubId; Nov-Dec 2025 files: item, itemcode, clubid). NULL when absent or not an object."""
    return (f"try_element_at(map_values(map_filter(try_variant_get({variant}, '$', 'map<string,variant>'), "
            f"(k, x) -> lower(k) = '{name}')), 1)")


def vpath(variant, path):
    """vget along a dotted path such as '$.promotionitems.promotionitem'."""
    for name in path.lstrip("$.").split("."):
        variant = vget(variant, name)
    return variant


def variant_array(variant, path):
    """Items under `path` of a JSON variant: an object becomes a 1-element list; no value gives an empty list."""
    v = vpath(variant, path)
    return (f"filter(coalesce(try_variant_get({v}, '$', 'array<variant>'), "
            f"array(try_variant_get({v}, '$', 'variant'))), x -> x IS NOT NULL)")


def promo_sql(keys, raw_view="raw0"):
    item_names = rename_sql(PROMO_ITEM_NAMES)

    def item_map(v):
        # the JSON's own field names -> scraper names; NO_BODY (an empty element) is dropped
        return (f"map_filter(transform_keys(try_cast({v} AS map<string,string>), (k, x) -> coalesce(element_at({item_names}, lower(k)), lower(k))), "
                f"(k, x) -> x IS NOT NULL AND x <> 'NO_BODY')")

    group_items = (
        f"flatten(transform(g_arr, gg -> transform({variant_array('gg', '$.promotionitems.promotionitem')}, it -> "
        f"map_concat(map_filter({item_map('gg')}, (k, x) -> k <> 'promotionitems'), {item_map('it')}))))"
    )
    present = set(keys)
    header = map_sql(with_dups(plain_pairs(PROMO_HEADER, present), present))
    groups_promo = map_sql(with_dups(PROMO_GROUPS_ONLY + PROMO_COMMON, present))
    flat_promo = map_sql(with_dups(PROMO_FLAT_ONLY + PROMO_COMMON, present))
    return f"""WITH {fill_sql(keys, raw_view).lstrip()},
js AS (
  SELECT *,
    try_parse_json(replace(groups, "'", '"')) AS gv,
    try_parse_json(replace(promotionitems, "'", '"')) AS pv,
    try_parse_json(replace(clubs, "'", '"')) AS cv,
    try_parse_json(replace(additionalrestrictions, "'", '"')) AS av
  FROM clean
),
parts AS (
  SELECT *,
    {variant_array('gv', '$.group')} AS g_arr,
    {variant_array('pv', '$.item')} AS p_arr,
    coalesce(array_join(try_variant_get({vget('cv', 'clubid')}, '$', 'array<string>'), '|'), try_variant_get({vget('cv', 'clubid')}, '$', 'string')) AS club_ids,
    try_variant_get({vget('av', 'additionaliscoupon')}, '$', 'string') AS av_coupon,
    try_variant_get({vget('av', 'additionalgiftcount')}, '$', 'string') AS av_gift,
    try_variant_get({vget('av', 'additionalistotal')}, '$', 'string') AS av_total,
    try_variant_get({vget('av', 'additionalisactive')}, '$', 'string') AS av_active
  FROM js
),
counted AS (
  SELECT *,
    size(g_arr) > 0 AS is_groups,
    CASE
      WHEN size(g_arr) > 0 THEN CAST(aggregate(g_arr, 0L, (acc, gg) -> acc + size({variant_array('gg', '$.promotionitems.promotionitem')})) AS INT)
      WHEN size(p_arr) > 0 THEN size(p_arr)
      WHEN itemcode IS NOT NULL THEN 1
      ELSE 0
    END AS items_in_promotion
  FROM parts
),
shaped AS (
  SELECT *,
    items_in_promotion > {STORE_WIDE_MIN_ITEMS} AS store_wide,
    CASE
      WHEN items_in_promotion > {STORE_WIDE_MIN_ITEMS} THEN array()
      WHEN is_groups THEN {group_items}
      WHEN size(p_arr) > 0 THEN transform(p_arr, it -> {item_map('it')})
      WHEN itemcode IS NOT NULL THEN array(map_filter(map('ItemCode', itemcode, 'ItemType', itemtype, 'IsGiftItem', isgiftitem), (k, x) -> x IS NOT NULL))
      ELSE array()
    END AS items
  FROM counted
)
SELECT {COMMON_SELECT},
  CAST(NULL AS STRING) AS declared_count,
  CAST(NULL AS INT) AS promotions_in_file,
  CAST(NULL AS INT) AS items_in_file,
  CAST(NULL AS STRING) AS parse_error,
  {header} AS header,
  CAST(mid - blk + 1 AS INT) AS promotion_seq,
  CASE WHEN is_groups THEN {groups_promo} ELSE {flat_promo} END AS promotion,
  item,
  current_timestamp() AS loaded_at,
  items_in_promotion,
  store_wide
FROM shaped
LATERAL VIEW OUTER explode(items) lv AS item"""


def build_sql(kind, keys, raw_view="raw0"):
    if kind == "promo":
        return promo_sql(keys, raw_view)
    return price_or_store_sql(kind, keys, raw_view)


def known_keys(kind):
    if kind == "price":
        return set(PRICE_HEADER) | set(PRICE_FIELDS) | {"file_name"}
    if kind == "store":
        return set(STORE_HEADER) | set(STORE_FIELDS) | {"file_name"}
    return set(PROMO_KNOWN)

# COMMAND ----------

from pyspark.sql import functions as F

# The batch: queue rows that task claim_batch marked running (for this job run), and carry this file kind
where = "status = 'running'" + (f" AND job_run_id = '{JOB_RUN_ID}'" if JOB_RUN_ID else "")
batch = [r for r in spark.sql(f"SELECT version, version_date, kinds FROM {QUEUE} WHERE {where} ORDER BY load_order").collect()
         if CFG["queue_kind"] in r.kinds.split(",")]
dates = sorted({str(r.version_date) for r in batch})
print(f"{KIND}: batch of {len(batch)} version(s) with this kind, dates {dates}")
if not dates:
    dbutils.notebook.exit("nothing to load")

# Retry safety: remove what an earlier, interrupted attempt of these versions may have written
in_list = ", ".join(f"DATE'{d}'" for d in dates)
spark.sql(f"DELETE FROM {CFG['table']} WHERE source = 'kaggle' AND collected_date IN ({in_list})")

patterns = [f"{KAGGLE_ROOT}/date={d}/chain=*/" for d in dates]
files = (spark.read.format("binaryFile").option("pathGlobFilter", CFG["pattern"]).load(patterns).select("path"))
new_paths = sorted(r.path for r in files.collect())
if PATH_CONTAINS:
    new_paths = [p for p in new_paths if PATH_CONTAINS in p]
print(f"{KIND}: {len(new_paths)} file(s) to load")
if not new_paths:
    dbutils.notebook.exit("nothing to load")

# COMMAND ----------

# Column names of every new file, from the header line of each (a cheap text scan, not a CSV parse)
header_rows = (spark.read.text(new_paths).where(F.col("value").startswith("found_folder,"))
               .select("value", F.col("_metadata.file_path").alias("p")).distinct().collect())
all_names = set()
renamed_logged = set()
for r in header_rows:
    orig = [c.rstrip("\r") for c in next(csv.reader([r.value]))]
    names = dedup_names(orig)
    assert len(names) < N_COLS, f"header with {len(names)} columns, raise N_COLS"
    all_names |= set(names)
    if r.p not in renamed_logged:
        for i, (o, n) in enumerate(zip(orig, names)):
            if n != o.lower():
                first = names.index(n.rsplit("_", 1)[0]) if n.rsplit("_", 1)[0] in names else None
                print(f"RENAMED repeated header name in {r.p}: column {i + 1} '{o}' -> '{n}'"
                      f" (first spelling: column {None if first is None else first + 1} '{None if first is None else orig[first]}')")
                renamed_logged.add(r.p)
all_names.discard("found_folder")
dups = sorted(n for n in all_names if dup_suffix(n) and dup_suffix(n)[0] in all_names)
unknown = sorted(n for n in all_names - known_keys(KIND) if n not in dups or dup_suffix(n)[0] not in known_keys(KIND))
print("repeated header names (kept in bronze as <name>_2 ...):", dups or "none")
print("columns without a scraper name (kept under their own lowercase name):", unknown or "none")
keys = sorted(all_names | known_keys(KIND))
sql_text = build_sql(KIND, keys)

# COMMAND ----------

schema = ", ".join(f"_c{i} STRING" for i in range(N_COLS))


def read_raw(paths):
    raw = (spark.read.schema(schema)
           .option("header", "false").option("multiLine", "true")
           .option("quote", '"').option("escape", '"')
           .option("lineSep", chr(10))
           .csv(paths)
           .selectExpr("*", "_metadata.file_path AS file_path", "_metadata.file_modification_time AS modified_at",
                       "monotonically_increasing_id() AS mid"))
    raw.createOrReplaceTempView("raw0")


def write_files(paths):
    read_raw(paths)
    spark.sql(sql_text).write.format("delta").mode("append").saveAsTable(CFG["table"])


def sql_str(text):
    return "'" + str(text).replace("\\", "\\\\").replace("'", "''") + "'"


def write_error_row(path, error):
    """One bronze row for a file that can't be loaded: parse_error set, everything else NULL (same shape as the scraper's)."""
    common = (f"{sql_str(path)} AS file_path, regexp_extract({sql_str(path)}, '([^/]+)$', 1) AS file_name, 'kaggle' AS source, "
              f"regexp_extract({sql_str(path)}, 'chain=([^/]+)', 1) AS chain, "
              f"to_date(regexp_extract({sql_str(path)}, 'date=([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1)) AS collected_date, "
              f"CAST(NULL AS TIMESTAMP) AS modified_at")
    err = f"{sql_str(error)} AS parse_error"
    hdr = "CAST(NULL AS MAP<STRING,STRING>)"
    if KIND == "price":
        body = (f"CAST(NULL AS STRING) AS declared_count, CAST(NULL AS INT) AS tags_in_file, {err}, {hdr} AS header, "
                f"{hdr} AS fields, current_timestamp() AS loaded_at")
    elif KIND == "store":
        body = f"CAST(NULL AS INT) AS tags_in_file, {err}, {hdr} AS header, {hdr} AS fields, current_timestamp() AS loaded_at"
    else:
        body = (f"CAST(NULL AS STRING) AS declared_count, CAST(NULL AS INT) AS promotions_in_file, CAST(NULL AS INT) AS items_in_file, "
                f"{err}, {hdr} AS header, CAST(NULL AS INT) AS promotion_seq, {hdr} AS promotion, {hdr} AS item, "
                f"current_timestamp() AS loaded_at, CAST(NULL AS INT) AS items_in_promotion, CAST(NULL AS BOOLEAN) AS store_wide")
    spark.sql(f"SELECT {common}, {body}").write.format("delta").mode("append").saveAsTable(CFG["table"])


total = 0
failed_files = []
for start in range(0, len(new_paths), FILES_PER_WRITE):
    chunk = new_paths[start:start + FILES_PER_WRITE]
    try:
        write_files(chunk)
    except Exception as chunk_exc:
        # Safety net: retry file by file; a file that fails alone is recorded as one row with parse_error
        print(f"CHUNK FAILED ({type(chunk_exc).__name__}), retrying its {len(chunk)} files one by one: {str(chunk_exc)[:300]}")
        for path in chunk:
            try:
                write_files([path])
            except Exception as file_exc:
                msg = f"{type(file_exc).__name__}: {str(file_exc)[:500]}"
                print(f"FILE FAILED, saved as one parse_error row: {path}: {msg}")
                write_error_row(path, msg)
                failed_files.append(path)
    total += len(chunk)
    print(f"written {total} of {len(new_paths)} files")

if failed_files:
    print(f"{len(failed_files)} file(s) saved as parse_error rows:", failed_files)
print(spark.sql(f"SELECT count(DISTINCT file_path) AS files, count(*) AS rows FROM {CFG['table']} WHERE source = 'kaggle'").first())
