# Databricks notebook source
import gzip, re
import xml.etree.ElementTree as ET
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType

BUCKET = "s3://supermarket-detective-raw-971422709739"
SOURCE = f"{BUCKET}/source=scraper/date=*/chain=KESHET/"
TABLE = "workspace.bronze.price_items"
TABLE_PATH = f"{BUCKET}/bronze/price_items"
CHECKPOINT = f"{BUCKET}/bronze/_checkpoints/price_items"

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  file_path       STRING,
  file_name       STRING,
  source          STRING,
  chain           STRING,
  collected_date  DATE,
  modified_at     TIMESTAMP,
  declared_count  STRING,
  tags_in_file    INT,
  header          MAP<STRING, STRING>,
  fields          MAP<STRING, STRING>,
  loaded_at       TIMESTAMP
) USING DELTA
LOCATION '{TABLE_PATH}'
""")

# COMMAND ----------

text_map = MapType(StringType(), StringType())

parsed_schema = StructType([
    StructField("declared_count", StringType()),
    StructField("tags_in_file", IntegerType()),
    StructField("header", text_map),
    StructField("items", ArrayType(text_map)),
])

def parse_price_file(content):
    xml_bytes = gzip.decompress(content)
    root = ET.fromstring(xml_bytes)
    items_element = root.find("Items")
    header = {el.tag: el.text for el in root if len(el) == 0}
    items = [{el.tag: el.text for el in item} for item in items_element.findall("Item")]
    tags = len(re.findall(rb"<Item[ >]", xml_bytes))
    return (items_element.get("Count"), tags, header, items)

parse_udf = F.udf(parse_price_file, parsed_schema)

# COMMAND ----------

raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "binaryFile")
       .option("cloudFiles.partitionColumns", "")
       .option("pathGlobFilter", "[Pp]rice[Ff]ull*.gz")
       .load(SOURCE))

bronze = (raw
    .withColumn("parsed", parse_udf("content"))
    .select(
        F.col("path").alias("file_path"),
        F.regexp_extract("path", r"([^/]+)$", 1).alias("file_name"),
        F.regexp_extract("path", r"source=([^/]+)", 1).alias("source"),
        F.regexp_extract("path", r"chain=([^/]+)", 1).alias("chain"),
        F.to_date(F.regexp_extract("path", r"date=(\d{4}-\d{2}-\d{2})", 1)).alias("collected_date"),
        F.col("modificationTime").alias("modified_at"),
        F.col("parsed.declared_count").alias("declared_count"),
        F.col("parsed.tags_in_file").alias("tags_in_file"),
        F.col("parsed.header").alias("header"),
        F.explode_outer("parsed.items").alias("fields"),
        F.current_timestamp().alias("loaded_at"),
    ))

(bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABLE)
    .awaitTermination())