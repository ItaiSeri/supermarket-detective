# Databricks notebook source
import gzip, re
import xml.etree.ElementTree as ET
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType

# Bronze is a MANAGED Delta table (Databricks storage in us-east-2, next to the compute); the checkpoint stays in S3.
BUCKET = "s3://supermarket-detective-raw-971422709739"
SOURCE = f"{BUCKET}/source=scraper/date=*/chain=*/"
TABLE = "workspace.bronze.stores"
CHECKPOINT = f"{BUCKET}/bronze/_checkpoints/stores"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  file_path       STRING,
  file_name       STRING,
  source          STRING,
  chain           STRING,
  collected_date  DATE,
  modified_at     TIMESTAMP,
  tags_in_file    INT,
  parse_error     STRING,
  header          MAP<STRING, STRING>,
  fields          MAP<STRING, STRING>,
  loaded_at       TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

text_map = MapType(StringType(), StringType())

store_schema = StructType([
    StructField("header", text_map),
    StructField("fields", text_map),
])

parsed_schema = StructType([
    StructField("tags_in_file", IntegerType()),
    StructField("parse_error", StringType()),
    StructField("stores", ArrayType(store_schema)),
])

def parse_store_file(content):
    try:
        xml_bytes = gzip.decompress(content)
        root = ET.fromstring(xml_bytes)
        if xml_bytes[:2] in (b"\xff\xfe", b"\xfe\xff"):
            count_bytes = xml_bytes.decode("utf-16").encode("utf-8")
        else:
            count_bytes = xml_bytes
        tags = len(re.findall(rb"<Store[ >]", count_bytes))

        stores = []

        def walk(element, above):
            own = {el.tag: el.text for el in element if len(el) == 0}
            if element.tag == "Store":
                stores.append((above, own))
                return
            for child in element:
                if len(child) > 0:
                    walk(child, {**above, **own})

        walk(root, {})
        return (tags, None, stores)
    except Exception as exc:
        return (None, f"{type(exc).__name__}: {exc}", [])

parse_udf = F.udf(parse_store_file, parsed_schema)

# COMMAND ----------

raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "binaryFile")
       .option("cloudFiles.partitionColumns", "")
       .option("pathGlobFilter", "[Ss]tores*.gz")
       .load(SOURCE))

bronze = (raw
    .withColumn("parsed", parse_udf("content"))
    .withColumn("store", F.explode_outer("parsed.stores"))
    .select(
        F.col("path").alias("file_path"),
        F.regexp_extract("path", r"([^/]+)$", 1).alias("file_name"),
        F.regexp_extract("path", r"source=([^/]+)", 1).alias("source"),
        F.regexp_extract("path", r"chain=([^/]+)", 1).alias("chain"),
        F.to_date(F.regexp_extract("path", r"date=(\d{4}-\d{2}-\d{2})", 1)).alias("collected_date"),
        F.col("modificationTime").alias("modified_at"),
        F.col("parsed.tags_in_file").alias("tags_in_file"),
        F.col("parsed.parse_error").alias("parse_error"),
        F.col("store.header").alias("header"),
        F.col("store.fields").alias("fields"),
        F.current_timestamp().alias("loaded_at"),
    ))

(bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABLE)
    .awaitTermination())