# Databricks notebook source
# Category pilot (G1c). Question: how well does Databricks' ai_classify sort Israeli supermarket products
# into 24 categories, how long does it take and what does it cost? 1,000 products, one table, no effect on gold.
# The final table silver.product_categories (filled in G1e) is a different table; this one is overwritten
# in every pilot run. Only mode 'pilot' exists so far.
import json, time

dbutils.widgets.text("mode", "pilot")
dbutils.widgets.text("sample_size", "1000")
mode = dbutils.widgets.get("mode")
sample_size = int(dbutils.widgets.get("sample_size"))
assert mode == "pilot", f"only mode 'pilot' is implemented, got {mode!r}"
half = sample_size // 2

# COMMAND ----------

# The 24 labels: key -> (Hebrew description given to the model, category_group, is_food).
# Keys follow the CBS price-index groups, merged where a shopper would not tell the groups apart.
# category_group: food | fresh_produce | nonfood. is_food is true for food and fresh_produce.
LABELS = {
    "bread_bakery":        ("לחם, לחמניות, פיתות, חלות, מאפים, עוגות ועוגיות מהמאפייה", "food"),
    "grains_pasta_legumes": ("קמח, אורז, פסטה, פתיתים, קטניות, דגני בוקר, קוסקוס", "food"),
    "meat_poultry":        ("בשר, עוף, הודו, נקניק ונקניקיות, טרי, קפוא או מעובד", "food"),
    "fish":                ("דגים ופירות ים, טונה ודגים משומרים", "food"),
    "dairy_eggs":          ("חלב, גבינות, יוגורט, שמנת, חמאה, מעדני חלב, ביצים, משקאות חלב ותחליפי חלב", "food"),
    "oils_fats":           ("שמן, מרגרינה, שומן לבישול", "food"),
    "sweets_snacks":       ("ממתקים, שוקולד, חטיפים מלוחים ומתוקים, עוגיות ועוגות ארוזות, גלידה, סוכר, דבש, ריבה וממרחים מתוקים", "food"),
    "coffee_tea":          ("קפה, תה, קקאו ומשקאות חמים להכנה", "food"),
    "soft_drinks":         ("מים, מיצים, משקאות קלים, סודה, משקאות אנרגיה", "food"),
    "alcohol":             ("יין, בירה, וודקה, ערק ומשקאות אלכוהוליים אחרים", "food"),
    "fresh_fruit":         ("פירות טריים", "fresh_produce"),
    "fresh_vegetables":    ("ירקות טריים ועשבי תיבול טריים", "fresh_produce"),
    "prepared_other_food": ("רטבים, תבלינים, שימורים, סלטים מוכנים, מרקים, מזון מוכן וקפוא, תוספות, ממרחים מלוחים, אגוזים ופירות יבשים", "food"),
    "baby_food":           ('תמ"ל, דייסות ומזון לתינוקות', "food"),
    "baby_care":           ("חיתולים, מגבונים, בקבוקים ומוצצים ומוצרי טיפוח לתינוק", "nonfood"),
    "cleaning_laundry":    ("חומרי ניקוי, אבקות ונוזלי כביסה, מרככים, נוזל כלים, טבליות למדיח, מטהרי אוויר", "nonfood"),
    "paper_disposables":   ("נייר טואלט, מגבות נייר, טישו, כלים חד פעמיים, נייר כסף ואפייה, שקיות אשפה ואחסון", "nonfood"),
    "personal_care":       ("שמפו, סבון, דאודורנט, משחת שיניים, מוצרי גילוח, קוסמטיקה, היגיינה נשית", "nonfood"),
    "health_pharma":       ("תרופות ללא מרשם, ויטמינים, תוספי תזונה, עזרה ראשונה", "nonfood"),
    "pet":                 ("מזון וציוד לחיות מחמד", "nonfood"),
    "home_kitchen":        ("כלי מטבח, כלי בית, כלי אחסון, מכשירי חשמל, טקסטיל לבית, כלי עבודה, נורות, סוללות, פריטי נוי", "nonfood"),
    "toys_stationery_seasonal": ("צעצועים, ציוד משרדי ולבית ספר, קישוטים, מוצרי חג ומסיבות, נרות", "nonfood"),
    "tobacco":             ("סיגריות, טבק ומוצרי עישון", "nonfood"),
    "other_nonfood":       ("כל מוצר שאינו מזון ואינו מתאים לקטגוריות האחרות", "nonfood"),
}
assert len(LABELS) == 24
labels_json = json.dumps({k: v[0] for k, v in LABELS.items()}, ensure_ascii=False)
INSTRUCTIONS = ("Product names from Israeli supermarket price files, in Hebrew, often abbreviated, with brand, "
                "size and unit. Choose the single category that best describes what the product is.")

# the mapping table, as a small temp view for the SQL below
spark.createDataFrame(
    [(k, v[1], v[1] in ("food", "fresh_produce")) for k, v in LABELS.items()],
    "category STRING, category_group STRING, is_food BOOLEAN").createOrReplaceTempView("label_map")

# COMMAND ----------

# Sample, deterministic (ordered by a hash of the key, so a rerun picks the same products):
#  stratum 'multi_chain' = active and in 3+ chains (the products marts will compare),
#  stratum 'other'       = active and in fewer chains.
# Active = last_seen within 14 days of the newest last_seen in silver.items.
# Input text: name | manufacturer (unless empty or a placeholder) | quantity + unit.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW pilot_sample AS
WITH cutoff AS (SELECT date_sub(max(last_seen), 14) AS d FROM workspace.silver.items),
active AS (
  SELECT i.* FROM workspace.silver.items i, cutoff c
  WHERE i.last_seen >= c.d AND i.item_name IS NOT NULL AND trim(i.item_name) <> ''
),
ranked AS (
  SELECT *, CASE WHEN chains_count >= 3 THEN 'multi_chain' ELSE 'other' END AS stratum,
         row_number() OVER (PARTITION BY CASE WHEN chains_count >= 3 THEN 'multi_chain' ELSE 'other' END
                            ORDER BY xxhash64(item_key)) AS rn
  FROM active
)
SELECT item_key, stratum,
       concat_ws(' | ',
         trim(item_name),
         CASE WHEN manufacturer_name IS NOT NULL AND length(trim(manufacturer_name)) >= 2
                   AND lower(trim(manufacturer_name)) NOT IN ('0', '-', '--', 'לא ידוע', 'unknown', 'null', 'none', 'n/a')
              THEN concat('יצרן: ', trim(manufacturer_name)) END,
         CASE WHEN quantity IS NOT NULL AND quantity > 0
              THEN concat(regexp_replace(CAST(CAST(quantity AS DOUBLE) AS STRING), '[.]0$', ''),
                          coalesce(concat(' ', nullif(trim(unit_of_measure), '')), '')) END
       ) AS input_text
FROM ranked WHERE rn <= {half}
""")
print(spark.sql("SELECT stratum, count(*) n FROM pilot_sample GROUP BY stratum").collect())

# COMMAND ----------

# The classification. ai_classify returns a VARIANT, so it cannot sit in a view: it is written to a table.
# Version 2.1 with confidence scores; the label argument is a JSON object (label -> Hebrew description).
labels_sql = labels_json.replace("\\", "\\\\").replace("'", "\\'")
instr_sql = INSTRUCTIONS.replace("'", "\\'")
t0 = time.time()
spark.sql(f"""
CREATE OR REPLACE TABLE workspace.silver.product_categories_pilot
COMMENT 'PILOT, not the final table. About 1,000 active products classified by ai_classify 2.1 into 24 labels. Overwritten by every pilot run. Final table: silver.product_categories.'
AS
WITH r AS (
  SELECT item_key, stratum, input_text,
         ai_classify(input_text, '{labels_sql}',
                     map('version', '2.1', 'enableConfidenceScores', 'true', 'instructions', '{instr_sql}')) AS res
  FROM pilot_sample
)
SELECT r.item_key, r.stratum, r.input_text,
       r.res:response[0].value::string            AS category,
       r.res:response[0].confidence_score::double AS confidence,
       m.category_group, m.is_food,
       r.res:error_message::string                AS error_message,
       CAST(r.res AS STRING)                      AS raw_result,
       r.res:metadata.version::string             AS model_version,
       current_timestamp()                        AS classified_at
FROM r LEFT JOIN label_map m ON m.category = r.res:response[0].value::string
""")
elapsed = time.time() - t0
print(f"classification statement wall-clock: {elapsed:.1f} s")

# COMMAND ----------

n, errs, nulls, bad = spark.sql("""
SELECT count(*), count(error_message), count_if(category IS NULL), count_if(category IS NOT NULL AND category_group IS NULL)
FROM workspace.silver.product_categories_pilot""").first()
print(f"pilot rows {n}, with error_message {errs}, category NULL {nulls}, category not in label list {bad}")
assert n > 0, "pilot table is empty"
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
print("job run id:", ctx.currentRunId().get().id() if ctx.currentRunId().isDefined() else None,
      "| classification s:", round(elapsed, 1))
