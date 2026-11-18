# Databricks notebook source
# Category pilot (G1c). Question: how well does Databricks' ai_classify sort Israeli supermarket products
# into 24 categories, how long does it take and what does it cost? 1,000 products, one table, no effect on gold.
# The final table silver.product_categories (filled in G1e) is a different table; this one is overwritten
# in every pilot run. mode 'eval' (round 2a) compares three methods on the hand-labelled
# databricks/seed/category_eval.csv and writes silver.product_categories_eval.
import json, time

dbutils.widgets.text("mode", "pilot")
dbutils.widgets.text("sample_size", "1000")
dbutils.widgets.text("job_run_id", "")  # the bundle passes {{job.run_id}}, so the run can be found in system.billing.usage
# mode 'full' (the real classification, resumable) uses these four
dbutils.widgets.text("model", "databricks-gpt-oss-120b")
dbutils.widgets.text("prompt_version", "v2")
dbutils.widgets.text("batch_size", "5000")
dbutils.widgets.text("max_minutes", "150")  # no new batch starts after this many minutes
dbutils.widgets.text("scope", "active")  # mode=full only: active (seen in the last 14 days) | history (all the others)
mode = dbutils.widgets.get("mode")
sample_size = int(dbutils.widgets.get("sample_size"))
assert mode in ("pilot", "eval", "full"), f"mode must be 'pilot', 'eval' or 'full', got {mode!r}"
half = sample_size // 2
run_eval = (mode == "eval")

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

# ---------------------------------------------------------------------------------------------
# mode 'eval' (round 2a): three methods on the hand-labelled test set, one table silver.product_categories_eval.
# The test set is databricks/seed/category_eval.csv (240 products; eval_set round1_review / new_blind; 6 are
# AMBIGUOUS and left out of the scores, not out of this table). The notebook ends here in eval mode.
# ---------------------------------------------------------------------------------------------
HINTS = """Product names from Israeli supermarket price files, in Hebrew, often abbreviated, with brand, size and unit. Choose the single category that best describes what the product is.
Rules:
- Kitchen tools and utensils named after a food (egg slicer, pepper grinder, juicer, pizza cutter) are home_kitchen, not food.
- Ice cream and ice pops (גלידה, ארטיק, שלגון, אסקימו, קרחון), jam and confiture, honey and sweet spreads are sweets_snacks.
- Plant-based drinks (rice, oat, soy, almond) and dairy desserts (מעדן, פודינג) are dairy_eggs.
- Cheese named only by its type (גאודה, סולוגוני, מוצרלה, פרמזן, צ'דר, פטה) is dairy_eggs.
- Baby bottles, pacifiers, diapers and baby wipes are baby_care; infant formula and baby purees are baby_food; children's toothpaste and shampoo are personal_care.
- Sanitary pads and tampons are personal_care. Trash bags and disposable plates, cups and bowls are paper_disposables.
- Candles of any kind, party supplies and holiday decorations are toys_stationery_seasonal.
- Spices and seasoning mixes, sauces, pickles and canned goods are prepared_other_food.
- Cigarette and tobacco brands can appear without the word cigarette (Winston, Marlboro, L&M, Kent, Parliament, Next, LD, Camel) and are tobacco.
- Bread and bakery products stay bread_bakery even when frozen (ciabatta, burekas, pita, pizza base).
- Laundry soap bars, dish soap and cleaning products are cleaning_laundry; body and hand soap are personal_care.
- Clothing, shoes, newspapers, shopping bags and delivery or service fees are other_nonfood."""


def sql_lit(text):
    # a Python string as a SQL string literal (backslash and quote escaped)
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def input_text_query(source, keep_cols, view_name):
    """SQL creating temp view `view_name` with keep_cols (comma list, may use aliases) plus input_text v2.
    `source` is a FROM clause where the items table is aliased `i`.
    Input text v2: longest name in `names`; manufacturer only if a real value; quantity + unit only if clean
    (unit not NULL / Unknown / 1, no '?' and no digits after dropping a leading per-100 prefix)."""
    outer = ", ".join(c.split(".")[-1].strip() for c in keep_cols.split(","))
    return f"""
    CREATE OR REPLACE TEMP VIEW {view_name} AS
    WITH j AS (
      SELECT {keep_cols},
             coalesce(nullif(trim(aggregate(i.names, '', (acc, x) -> CASE WHEN length(x) > length(acc) THEN x ELSE acc END)), ''),
                      trim(i.item_name)) AS nm,
             trim(i.manufacturer_name) AS mf,
             i.quantity AS qty,
             trim(regexp_replace(trim(i.unit_of_measure), '^100[ ]+', '')) AS unit
      FROM {source}
    )
    SELECT {outer},
           concat_ws(' | ', nm,
             CASE WHEN mf IS NOT NULL AND length(mf) > 1
                       AND lower(mf) NOT IN (',', 'לא ידוע', 'כללי', 'שונות', 'unknown')
                  THEN concat('יצרן: ', mf) END,
             CASE WHEN qty IS NOT NULL AND qty > 0 AND unit IS NOT NULL AND unit <> ''
                       AND lower(unit) NOT IN ('unknown', '1') AND instr(unit, '?') = 0 AND unit NOT RLIKE '[0-9]'
                  THEN concat(regexp_replace(CAST(CAST(qty AS DOUBLE) AS STRING), '[.]0$', ''), ' ', unit) END
           ) AS input_text
    FROM j"""


# the ai_query prompt (prompt version v2) and JSON schema, shared by mode eval and mode full
label_list_txt = "\n".join(f"- {k}: {v[0]}" for k, v in LABELS.items())
enum_json = json.dumps(list(LABELS.keys()))
schema_json = ('{"type":"json_schema","json_schema":{"name":"category","strict":true,"schema":{"type":"object",'
               '"properties":{"category":{"type":"string","enum":' + enum_json + '},"confidence":{"type":"number"}},'
               '"required":["category","confidence"],"additionalProperties":false}}}')
prompt_prefix = (HINTS + "\n\nThe categories (key: Hebrew description):\n" + label_list_txt +
                 "\n\nReturn only JSON like {\"category\": \"<one of the category keys above>\", \"confidence\": <number between 0 and 1>}.\n\nProduct: ")



if run_eval:
    import pandas as pd
    eval_pd = pd.read_csv("../seed/category_eval.csv", dtype=str)
    assert len(eval_pd) == 240, f"expected 240 eval rows, got {len(eval_pd)}"
    spark.createDataFrame(eval_pd[["item_key", "eval_set", "name", "true_category"]]).createOrReplaceTempView("eval_csv")

    # Input text v2 (see input_text_query above)
    spark.sql(input_text_query("eval_csv e LEFT JOIN workspace.silver.items i ON i.item_key = e.item_key",
                               "e.item_key, e.eval_set", "eval_input"))
    n_match = spark.sql("SELECT count(*) FROM eval_csv e JOIN workspace.silver.items i ON i.item_key = e.item_key").first()[0]
    n_input = spark.sql("SELECT count(*) FROM eval_input WHERE input_text IS NOT NULL AND trim(input_text) <> ''").first()[0]
    assert n_match == 240 and n_input == 240, f"eval join: {n_match} items matched, {n_input} non-empty inputs, expected 240"
    # materialise the inputs once, so the three methods see exactly the same text
    spark.sql("CREATE OR REPLACE TABLE workspace.silver.product_categories_eval_input AS SELECT * FROM eval_input")

    # results go into one Delta table, one INSERT per method, so each method's statement is timed on its own
    spark.sql("""CREATE OR REPLACE TABLE workspace.silver.product_categories_eval (
        item_key STRING, eval_set STRING, method STRING, input_text STRING, category STRING,
        confidence DOUBLE, raw_result STRING, run_at TIMESTAMP)
        COMMENT 'ROUND 2a eval: three category methods on the 240 hand-labelled products of databricks/seed/category_eval.csv. Overwritten by every mode=eval run.'""")
    timings = {}

    t0 = time.time()
    spark.sql(f"""
    INSERT INTO workspace.silver.product_categories_eval
    WITH r AS (
      SELECT item_key, eval_set, input_text,
             ai_classify(input_text, {sql_lit(labels_json)},
                         map('version', '2.1', 'enableConfidenceScores', 'true', 'instructions', {sql_lit(HINTS)})) AS res
      FROM workspace.silver.product_categories_eval_input)
    SELECT item_key, eval_set, 'ai_classify_v2', input_text,
           res:response[0].value::string, res:response[0].confidence_score::double, CAST(res AS STRING), current_timestamp()
    FROM r""")
    timings["ai_classify_v2"] = time.time() - t0
    print(f"ai_classify_v2: {timings['ai_classify_v2']:.1f} s")

    for method, endpoint in [("gpt_oss_120b", "databricks-gpt-oss-120b"), ("qwen35_122b", "databricks-qwen35-122b-a10b")]:
        t0 = time.time()
        spark.sql(f"""
        INSERT INTO workspace.silver.product_categories_eval
        WITH r AS (
          SELECT item_key, eval_set, input_text,
                 ai_query('{endpoint}', concat({sql_lit(prompt_prefix)}, input_text),
                          responseFormat => {sql_lit(schema_json)}, failOnError => false) AS res
          FROM workspace.silver.product_categories_eval_input),
        p AS (SELECT *, try_parse_json(res.result) AS j FROM r)
        SELECT item_key, eval_set, '{method}', input_text,
               j:category::string, j:confidence::double,
               CAST(res AS STRING), current_timestamp()
        FROM p""")
        timings[method] = time.time() - t0
        print(f"{method}: {timings[method]:.1f} s")

    valid = ",".join(f"'{k}'" for k in LABELS)
    print("method, rows, category set, category not in the 24 labels:",
          spark.sql(f"SELECT method, count(*), count(category), count_if(category IS NOT NULL AND category NOT IN ({valid})) "
                    "FROM workspace.silver.product_categories_eval GROUP BY method").collect())
    dbutils.notebook.exit(json.dumps({"mode": "eval", "seconds": {k: round(v, 1) for k, v in timings.items()}}))

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# mode 'full': classify every active, non-Be product with ai_query, in batches, resumable.
# Results: silver.product_categories_ai (one row per item_key + model + prompt_version, successes only, never replaced).
# A rerun picks up the products that have no row yet for (model, prompt_version): nothing is done twice.
# Batch log: silver.product_categories_ai_batches.
# ---------------------------------------------------------------------------------------------
if mode == "full":
    model = dbutils.widgets.get("model")
    prompt_version = dbutils.widgets.get("prompt_version")
    batch_size = int(dbutils.widgets.get("batch_size"))
    max_minutes = float(dbutils.widgets.get("max_minutes"))
    run_start = time.time()
    RES = "workspace.silver.product_categories_ai"
    LOG = "workspace.silver.product_categories_ai_batches"
    SCOPE = "workspace.silver.product_categories_ai_scope"
    TMP = "workspace.silver.product_categories_ai_tmp"
    valid = ",".join(f"'{k}'" for k in LABELS)

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {RES} (
        item_key STRING, model STRING, prompt_version STRING, input_text STRING, category STRING,
        raw_result STRING, error_message STRING, batch_id STRING, classified_at TIMESTAMP)
        COMMENT 'ai_query category per product (successes only, never replaced; a failed product has no row and is retried by the next run). One row per item_key + model + prompt_version. Written by notebook silver_product_categories.py mode=full.'""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {LOG} (
        batch_id STRING, model STRING, prompt_version STRING, started_at TIMESTAMP, seconds DOUBLE,
        rows_sent BIGINT, rows_ok BIGINT, rows_error BIGINT, sample_error STRING)
        COMMENT 'One row per ai_query batch of mode=full.'""")

    # Scope, built once per run, minus products sold only in Shufersal Be stores (sub_chain_id 5; Be is out of
    # gold). Most-shared products first.
    #   scope=active:  last_seen within 14 days of the newest (prices seen in the last 14 days)
    #   scope=history: every other product (seen only earlier, mostly from the Kaggle backfill), any date
    # Results go to the same table; a product is never classified twice for one model + prompt_version.
    scope = dbutils.widgets.get("scope")
    assert scope in ("active", "history"), f"scope must be active or history, got {scope}"
    if scope == "active":
        items_where = "i.last_seen >= (SELECT date_sub(max(last_seen), 14) FROM workspace.silver.items)"
        ph_where = "ph.last_seen >= date_sub(current_date(), 14) AND"
    else:
        items_where = "i.last_seen < (SELECT date_sub(max(last_seen), 14) FROM workspace.silver.items)"
        ph_where = ""
    spark.sql(input_text_query(
        f"""workspace.silver.items i
        WHERE {items_where}
          AND i.item_name IS NOT NULL AND trim(i.item_name) <> ''
          AND i.item_key IN (
            SELECT workspace.silver.item_key(ph.chain, ph.item_code, ph.item_type)
            FROM workspace.silver.price_history ph
            LEFT JOIN workspace.silver.stores s ON s.chain = ph.chain AND s.store_id = ph.store_id
            WHERE {ph_where} NOT (ph.chain = 'SHUFERSAL' AND coalesce(s.sub_chain_id, '') = '5'))""",
        "i.item_key, i.chains_count", "full_scope_v"))
    spark.sql(f"CREATE OR REPLACE TABLE {SCOPE} AS SELECT * FROM full_scope_v "
              "WHERE input_text IS NOT NULL AND trim(input_text) <> ''")
    n_scope = spark.table(SCOPE).count()
    print(f"scope {scope}: {n_scope} products; model {model}, prompt {prompt_version}, batch {batch_size}, max {max_minutes} min")

    def remaining():
        return spark.sql(f"""SELECT count(*) FROM {SCOPE} s WHERE NOT EXISTS (
            SELECT 1 FROM {RES} r WHERE r.item_key = s.item_key AND r.model = {sql_lit(model)}
              AND r.prompt_version = {sql_lit(prompt_version)})""").first()[0]

    stop_reason = "nothing left"
    left = remaining()
    n_batches = 0
    done_rows = 0
    total_secs = 0.0
    while left > 0:
        if (time.time() - run_start) / 60 > max_minutes:
            stop_reason = f"max_minutes ({max_minutes}) passed, {left} products left"
            break
        batch_id = f"{time.strftime('%Y%m%d-%H%M%S')}-{n_batches}"
        started = time.time()
        spark.sql(f"""CREATE OR REPLACE TABLE {TMP} AS
          WITH b AS (
            SELECT s.item_key, s.input_text FROM {SCOPE} s
            WHERE NOT EXISTS (SELECT 1 FROM {RES} r WHERE r.item_key = s.item_key AND r.model = {sql_lit(model)}
                                AND r.prompt_version = {sql_lit(prompt_version)})
            ORDER BY s.chains_count DESC, xxhash64(s.item_key) LIMIT {batch_size}),
          r AS (
            SELECT item_key, input_text,
                   ai_query({sql_lit(model)}, concat({sql_lit(prompt_prefix)}, input_text),
                            responseFormat => {sql_lit(schema_json)}, failOnError => false) AS res
            FROM b)
          SELECT item_key, input_text, try_parse_json(res.result):category::string AS category,
                 CAST(res.result AS STRING) AS raw_result, res.errorMessage AS error_message
          FROM r""")
        sent, ok = spark.sql(f"SELECT count(*), count_if(category IN ({valid})) FROM {TMP}").first()
        sample_err = spark.sql(f"""SELECT coalesce(max(error_message), max(concat('bad category: ', raw_result)))
            FROM {TMP} WHERE category IS NULL OR category NOT IN ({valid})""").first()[0]
        spark.sql(f"""INSERT INTO {RES}
          SELECT item_key, {sql_lit(model)}, {sql_lit(prompt_version)}, input_text, category, raw_result, error_message,
                 {sql_lit(batch_id)}, current_timestamp()
          FROM {TMP} WHERE category IN ({valid})""")
        secs = time.time() - started
        spark.createDataFrame(
            [(batch_id, model, prompt_version, float(secs), int(sent), int(ok), int(sent - ok), sample_err[:2000] if sample_err else None)],
            "batch_id STRING, model STRING, prompt_version STRING, seconds DOUBLE, rows_sent BIGINT, rows_ok BIGINT, rows_error BIGINT, sample_error STRING"
        ).createOrReplaceTempView("batch_row")
        spark.sql(f"""INSERT INTO {LOG} SELECT batch_id, model, prompt_version,
                      timestampadd(SECOND, CAST(-seconds AS INT), current_timestamp()), seconds,
                      rows_sent, rows_ok, rows_error, sample_error FROM batch_row""")
        n_batches += 1
        done_rows += ok
        total_secs += secs
        left = remaining()
        rate = ok / secs if secs else 0
        eta = (left / (done_rows / total_secs) / 60) if done_rows else float("nan")
        print(f"batch {batch_id}: sent {sent}, ok {ok}, errors {sent - ok}, {secs:.0f} s, {rate:.1f} rows/s; "
              f"left {left}, estimated {eta:.0f} min at the average rate")
        if sent == 0 or (sent - ok) > 0.2 * sent:
            raise Exception(f"batch {batch_id}: {sent - ok} of {sent} rows failed (over 20%), stopping. "
                            f"Sample error: {sample_err}. Saved batches stay; rerun to resume.")

    summary = {"mode": "full", "scope": scope, "model": model, "batches": n_batches, "rows_ok": int(done_rows), "left": left, "stopped": stop_reason}
    print(summary)
    dbutils.notebook.exit(json.dumps(summary))

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
run_id = dbutils.widgets.get("job_run_id")
print("job run id:", run_id, "| classification s:", round(elapsed, 1))
# the result is also the notebook output, so a caller can read it from the Jobs API
dbutils.notebook.exit(json.dumps({"job_run_id": run_id, "rows": n, "errors": errs, "seconds": round(elapsed, 1)}))
