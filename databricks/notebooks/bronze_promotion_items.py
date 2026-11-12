# Databricks notebook source
import gzip, re
import xml.etree.ElementTree as ET
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType, BooleanType

# Bronze is a MANAGED Delta table (Databricks storage in us-east-2, next to the compute); the checkpoint stays in S3.
BUCKET = "s3://supermarket-detective-raw-971422709739"
SOURCE = f"{BUCKET}/source=scraper/date=*/chain=*/"
TABLE = "workspace.bronze.promotion_items"

# A promotion that lists MORE than this many products in one file is "store-wide" (a basket-level offer such as a
# coupon or a delivery fee that carries the whole catalog). Measured 2026-11-12: real product deals list at most
# about 5,200 products, store-wide offers start at about 11,500. Such a promotion is written as ONE row with
# item NULL; the list is never turned into rows (raw in S3 keeps it). Silver reads it into store_wide_promotions.
STORE_WIDE_MIN_ITEMS = 10000
CHECKPOINT = f"{BUCKET}/bronze/_checkpoints/promotion_items"

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  file_path          STRING,
  file_name          STRING,
  source             STRING,
  chain              STRING,
  collected_date     DATE,
  modified_at        TIMESTAMP,
  declared_count     STRING,
  promotions_in_file INT,
  items_in_file      INT,
  parse_error        STRING,
  header             MAP<STRING, STRING>,
  promotion_seq      INT,
  promotion          MAP<STRING, STRING>,
  item               MAP<STRING, STRING>,
  loaded_at          TIMESTAMP,
  items_in_promotion INT,
  store_wide         BOOLEAN
) USING DELTA
""")

# COMMAND ----------

text_map = MapType(StringType(), StringType())

parsed_schema = StructType([
    StructField("declared_count", StringType()),
    StructField("promotions_in_file", IntegerType()),
    StructField("items_in_file", IntegerType()),
    StructField("parse_error", StringType()),
    StructField("header", text_map),
    StructField("promotions", ArrayType(StructType([
        StructField("seq", IntegerType()),
        StructField("fields", text_map),
        StructField("items", ArrayType(text_map)),
        StructField("items_in_promotion", IntegerType()),
        StructField("store_wide", BooleanType()),
    ]))),
])

def flatten_promotion(element, out):
    # Every value of the promotion except its items, at any depth. The flat layout
    # nests some of them (Clubs/ClubId, AdditionalRestrictions/...). A tag that
    # repeats, such as several ClubId values, is kept with its values joined by |.
    for child in element:
        if child.tag in ("Groups", "PromotionItems"):
            continue
        if len(child):
            flatten_promotion(child, out)
        elif child.tag in out:
            out[child.tag] = f"{out[child.tag]}|{child.text}"
        else:
            out[child.tag] = child.text
    return out

def count_items(promotion):
    # Counted from the tags alone, before any item is turned into a dict, so a huge list costs almost nothing.
    n = len(promotion.findall("PromotionItems/Item"))
    for group in promotion.findall("Groups/Group"):
        n += len(group.findall("PromotionItems/PromotionItem"))
    return n

def promotion_items(promotion):
    # Two layouts exist, sometimes within one chain:
    #   Groups/Group/PromotionItems/PromotionItem: the discount is on each item.
    #   PromotionItems/Item: the item has only its code; the discount is on the promotion.
    items = []
    for group in promotion.findall("Groups/Group"):
        group_fields = {el.tag: el.text for el in group if len(el) == 0}
        for item in group.findall("PromotionItems/PromotionItem"):
            items.append({**group_fields, **{el.tag: el.text for el in item}})
    for item in promotion.findall("PromotionItems/Item"):
        items.append({el.tag: el.text for el in item})
    return items

def parse_promotion(seq, p):
    n = count_items(p)
    store_wide = n > STORE_WIDE_MIN_ITEMS
    return (seq, flatten_promotion(p, {}), [] if store_wide else promotion_items(p), n, store_wide)

def parse_promo_file(content):
    try:
        xml_bytes = gzip.decompress(content)
        root = ET.fromstring(xml_bytes)
        header = {el.tag: el.text for el in root if len(el) == 0}
        container = root.find("Promotions")
        promotions = [
            parse_promotion(seq, p)
            for seq, p in enumerate(root.findall("Promotions/Promotion"), start=1)
        ]
        if xml_bytes[:2] in (b"\xff\xfe", b"\xfe\xff"):
            count_bytes = xml_bytes.decode("utf-16").encode("utf-8")
        else:
            count_bytes = xml_bytes
        promotion_tags = len(re.findall(rb"<Promotion[ />]", count_bytes))
        item_tags = len(re.findall(rb"<(?:PromotionItem|Item)[ />]", count_bytes))
        declared = container.get("Count") if container is not None else None
        return (declared, promotion_tags, item_tags, None, header, promotions)
    except Exception as exc:
        return (None, None, None, f"{type(exc).__name__}: {exc}", None, [])

parse_udf = F.udf(parse_promo_file, parsed_schema)

# COMMAND ----------

raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "binaryFile")
       .option("cloudFiles.partitionColumns", "")
       .option("cloudFiles.maxFilesPerTrigger", 100)
       .option("pathGlobFilter", "[Pp]romo[Ff]ull*.gz")
       .load(SOURCE))

# Same batch shape as the price load: about 5 files' parsed content in Python at once.
parsed = (raw
    .repartition(20)
    .withColumn("parsed", parse_udf("content"))
    .select(
        F.col("path").alias("file_path"),
        F.regexp_extract("path", r"([^/]+)$", 1).alias("file_name"),
        F.regexp_extract("path", r"source=([^/]+)", 1).alias("source"),
        F.regexp_extract("path", r"chain=([^/]+)", 1).alias("chain"),
        F.to_date(F.regexp_extract("path", r"date=(\d{4}-\d{2}-\d{2})", 1)).alias("collected_date"),
        F.col("modificationTime").alias("modified_at"),
        F.col("parsed.declared_count").alias("declared_count"),
        F.col("parsed.promotions_in_file").alias("promotions_in_file"),
        F.col("parsed.items_in_file").alias("items_in_file"),
        F.col("parsed.parse_error").alias("parse_error"),
        F.col("parsed.header").alias("header"),
        F.explode_outer("parsed.promotions").alias("p"),
    ))

# explode_outer keeps a file with no promotions, a promotion with no items, and a store-wide promotion (items
# emptied above) as one row.
bronze = parsed.select(
    "file_path", "file_name", "source", "chain", "collected_date", "modified_at",
    "declared_count", "promotions_in_file", "items_in_file", "parse_error", "header",
    F.col("p.seq").alias("promotion_seq"),
    F.col("p.fields").alias("promotion"),
    F.explode_outer("p.items").alias("item"),
    F.current_timestamp().alias("loaded_at"),
    F.col("p.items_in_promotion").alias("items_in_promotion"),
    F.col("p.store_wide").alias("store_wide"),
)

(bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABLE)
    .awaitTermination())
