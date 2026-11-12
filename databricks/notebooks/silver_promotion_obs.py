# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.promotion_obs (
  chain STRING,
  source STRING,
  chain_id STRING,
  sub_chain_id STRING,
  store_id STRING,
  promotion_id STRING,
  group_id STRING,
  item_code STRING,
  item_type STRING,
  layout STRING,
  description STRING,
  start_at TIMESTAMP,
  end_at TIMESTAMP,
  promotion_updated_at TIMESTAMP,
  reward_type STRING,
  min_qty DECIMAL(12,3),
  max_qty DECIMAL(12,3),
  discounted_price DECIMAL(12,2),
  discounted_price_per_unit DECIMAL(14,4),
  discount_rate DECIMAL(12,4),
  discount_type STRING,
  min_purchase_amount DECIMAL(12,2),
  is_weighted BOOLEAN,
  is_gift_item BOOLEAN,
  allow_multiple_discounts BOOLEAN,
  is_coupon BOOLEAN,
  club_code STRING,
  club_raw STRING,
  file_date DATE,
  file_time STRING,
  source_file STRING,
  collected_date DATE,
  modified_at TIMESTAMP,
  bronze_loaded_at TIMESTAMP
)
CLUSTER BY (file_date, chain)
""")

# Separate from silver.quarantine: the price checks in silver_dq count that table's rows.
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.promotion_quarantine (
  row_key STRING,
  reject_reason STRING,
  chain STRING,
  source STRING,
  chain_id STRING,
  store_id STRING,
  promotion_id STRING,
  item_code STRING,
  price_raw STRING,
  parse_error STRING,
  file_date DATE,
  source_file STRING,
  collected_date DATE,
  modified_at TIMESTAMP,
  bronze_loaded_at TIMESTAMP,
  header MAP<STRING, STRING>,
  promotion MAP<STRING, STRING>,
  item MAP<STRING, STRING>,
  quarantined_at TIMESTAMP
)
""")

# COMMAND ----------

last_load = spark.sql("""
SELECT coalesce(max(bronze_loaded_at), TIMESTAMP'1900-01-01') AS t
FROM workspace.silver.promotion_obs
""").first()["t"]

# Optional one-off re-read after a rule change: job parameter reload_after (a timestamp)
# moves the watermark back. Safe to re-read, because both MERGEs skip rows already saved.
dbutils.widgets.text("reload_after", "")
if dbutils.widgets.get("reload_after"):
    last_load = dbutils.widgets.get("reload_after")

print("Reading bronze rows loaded after", last_load)

# COMMAND ----------

classified = spark.sql("""
WITH expected AS (
  SELECT chain, min(chain_id) AS expected_chain_id
  FROM workspace.silver.stores
  GROUP BY chain
),
base AS (
  SELECT
    b.chain,
    b.source,
    -- Kaggle's Victory and Mahsani Ashuk promo files of 2025 carry a cut-off ChainID ('72906').
    -- The original file name inside them (PromoFull<13-digit chain id>-...) still has the full ID,
    -- so a header ID shorter than 13 digits is replaced by the file name's; the wrong_chain
    -- check below still compares it with the folder's chain.
    CASE
      WHEN length(trim(coalesce(b.header['ChainID'], b.header['ChainId']))) < 13
       AND regexp_extract(b.file_name, '^Promo(?:Full)?([0-9]{13})-', 1) <> ''
      THEN regexp_extract(b.file_name, '^Promo(?:Full)?([0-9]{13})-', 1)
      ELSE trim(coalesce(b.header['ChainID'], b.header['ChainId']))
    END AS chain_id,
    trim(coalesce(b.header['SubChainID'], b.header['SubChainId'])) AS sub_chain_id,
    regexp_replace(trim(coalesce(b.header['StoreID'], b.header['StoreId'])), '^0+(?=.)', '') AS store_id,
    -- Keshet publishes the same promotion as 1145634 and as 0001145634, depending on the layout.
    nullif(regexp_replace(trim(coalesce(b.promotion['PromotionID'], b.promotion['PromotionId'])), '^0+(?=.)', ''), '') AS promotion_id,
    -- The flat layout has no groups: the whole promotion counts as group 1.
    coalesce(nullif(trim(b.item['GroupID']), ''), '1') AS group_id,
    nullif(trim(b.item['ItemCode']), '') AS item_code,
    nullif(trim(b.item['ItemType']), '') AS item_type,
    CASE WHEN b.promotion['PromotionId'] IS NOT NULL THEN 'flat' ELSE 'groups' END AS layout,
    nullif(trim(b.promotion['PromotionDescription']), '') AS description,
    try_cast(replace(replace(trim(coalesce(b.promotion['PromotionStartDateTime'],
      concat(b.promotion['PromotionStartDate'], ' ', coalesce(b.promotion['PromotionStartHour'], '00:00:00')))), 'T', ' '), '/', '-') AS TIMESTAMP) AS start_at,
    try_cast(replace(replace(trim(coalesce(b.promotion['PromotionEndDateTime'],
      concat(b.promotion['PromotionEndDate'], ' ', coalesce(b.promotion['PromotionEndHour'], '23:59:59')))), 'T', ' '), '/', '-') AS TIMESTAMP) AS end_at,
    try_cast(replace(replace(trim(coalesce(b.promotion['PromotionUpdateTime'], b.promotion['PromotionUpdateDate'])), 'T', ' '), '/', '-') AS TIMESTAMP) AS promotion_updated_at,
    -- The Groups layout has these on the product, the flat layout on the promotion.
    nullif(trim(coalesce(b.item['RewardType'], b.promotion['RewardType'])), '') AS reward_type,
    try_cast(trim(coalesce(b.item['MinQty'], b.promotion['MinQty'])) AS DECIMAL(12,3)) AS min_qty,
    try_cast(trim(coalesce(b.item['MaxQty'], b.promotion['MaxQty'], b.promotion['MAXQTY'])) AS DECIMAL(12,3)) AS max_qty,
    try_cast(trim(coalesce(b.item['DiscountedPrice'], b.promotion['DiscountedPrice'])) AS DECIMAL(12,2)) AS discounted_price,
    try_cast(trim(coalesce(b.item['DiscountedPricePerMida'], b.promotion['DiscountedPricePerMida'])) AS DECIMAL(14,4)) AS discounted_price_per_unit,
    try_cast(trim(coalesce(b.item['DiscountRate'], b.promotion['DiscountRate'])) AS DECIMAL(12,4)) AS discount_rate,
    nullif(trim(coalesce(b.item['DiscountType'], b.promotion['DiscountType'])), '') AS discount_type,
    try_cast(trim(coalesce(b.item['MinPurchaseAmount'], b.promotion['MinPurchaseAmnt'])) AS DECIMAL(12,2)) AS min_purchase_amount,
    trim(coalesce(b.item['bIsWeighted'], b.promotion['IsWeightedPromo'])) = '1' AS is_weighted,
    -- Only the flat layout's product-level flag is used; the promotion-level one holds values like 3 or 8.
    trim(b.item['IsGiftItem']) = '1' AS is_gift_item,
    trim(b.promotion['AllowMultipleDiscounts']) = '1' AS allow_multiple_discounts,
    trim(b.promotion['AdditionalIsCoupon']) = '1' AS is_coupon,
    nullif(regexp_extract(trim(coalesce(b.promotion['ClubID'], b.promotion['ClubId'])), '([0-9]+)', 1), '') AS club_code,
    nullif(trim(coalesce(b.promotion['ClubID'], b.promotion['ClubId'])), '') AS club_raw,
    to_date(try_to_timestamp(regexp_extract(b.file_name, '-(20[0-9]{6})', 1), 'yyyyMMdd')) AS file_date,
    rpad(regexp_replace(regexp_extract(b.file_name, '-(20[0-9]{6}-?[0-9]{4,6})', 1), '-', ''), 14, '0') AS file_time,
    b.file_path AS source_file,
    b.collected_date,
    b.modified_at,
    b.loaded_at AS bronze_loaded_at,
    b.parse_error,
    b.promotion_seq,
    b.header,
    b.promotion,
    b.item,
    coalesce(b.item['DiscountedPrice'], b.promotion['DiscountedPrice']) AS price_raw,
    e.expected_chain_id
  FROM workspace.bronze.promotion_items b
  LEFT JOIN expected e ON b.chain = e.chain
  WHERE b.loaded_at > :last_load
)
SELECT *,
  CASE
    WHEN parse_error IS NOT NULL THEN 'file_not_readable'
    WHEN promotion IS NULL THEN 'file_has_no_promotions'
    WHEN NOT (chain_id <=> expected_chain_id) THEN 'wrong_chain'
    WHEN file_date IS NULL THEN 'no_file_date'
    WHEN store_id IS NULL OR store_id = '' THEN 'no_store_id'
    WHEN promotion_id IS NULL THEN 'no_promotion_id'
    WHEN item_code IS NULL THEN 'no_item_code'
    WHEN discounted_price < 0 THEN 'price_negative'
  END AS reject_reason
FROM base
""", args={"last_load": last_load})

classified.createOrReplaceTempView("classified")

# COMMAND ----------

# Rejects first: saving promotions moves the watermark forward.
spark.sql("""
MERGE INTO workspace.silver.promotion_quarantine AS t
USING (
  SELECT
    sha2(concat_ws('|', source_file, promotion_seq, to_json(item), parse_error), 256) AS row_key,
    reject_reason, chain, source, chain_id, store_id, promotion_id, item_code, price_raw,
    parse_error, file_date, source_file, collected_date, modified_at,
    bronze_loaded_at, header, promotion, item,
    current_timestamp() AS quarantined_at
  FROM classified
  WHERE reject_reason IS NOT NULL
) AS s
ON t.row_key = s.row_key
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# Inside one file a product can repeat in the same group, typically once at 0.00 and
# once at its real price (Shufersal), so a positive price is preferred.
spark.sql("""
MERGE INTO workspace.silver.promotion_obs AS t
USING (
  SELECT * EXCEPT (parse_error, promotion_seq, header, promotion, item, price_raw, expected_chain_id, reject_reason)
  FROM classified
  WHERE reject_reason IS NULL
  QUALIFY row_number() OVER (
    PARTITION BY chain, store_id, promotion_id, group_id, item_code, file_date
    ORDER BY CASE source WHEN 'scraper' THEN 1 ELSE 2 END, file_time DESC, modified_at DESC,
             CASE WHEN discounted_price > 0 THEN 1 ELSE 2 END,
             discounted_price ASC NULLS LAST, discount_rate DESC NULLS LAST, source_file, to_json(item)
  ) = 1
) AS s
ON  t.file_date = s.file_date
AND t.chain = s.chain
AND t.store_id = s.store_id
AND t.promotion_id = s.promotion_id
AND t.group_id = s.group_id
AND t.item_code = s.item_code
WHEN MATCHED AND (
     (t.source = 'kaggle' AND s.source = 'scraper')
  OR (t.source = s.source AND s.file_time > t.file_time)
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
