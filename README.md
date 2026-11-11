# Supermarket Detective

Israeli supermarket prices as a lakehouse: every chain's legally required price and promo files, kept as full price history, with a Power BI report on top.
Questions it answers: is Eilat really 15.3% cheaper (no VAT)? Which "promotions" are not cheaper than the regular price was? Which products shrank while the price stayed? How do prices move before holidays?

Stack: AWS (Tel Aviv region, ingestion) → Databricks Free Edition (Unity Catalog, Auto Loader, Lakeflow declarative pipelines, Jobs, bundles) → Power BI. Everything scheduled runs in the cloud; the laptop is only for writing code and building the Power BI report.

## Architecture

```
AWS il-central-1 (Tel Aviv)               DATABRICKS FREE EDITION                        POWER BI
---------------------------               ------------------------------------------     ---------------
EventBridge Scheduler (daily 09:00)       External location supermarket_raw_scraper      Desktop (authoring),
  -> Step Functions state machine           reads S3 directly, no copy:                  Import from the SQL
     Map state, one item per chain +        source=scraper|kaggle/date=/chain=/           warehouse
     file type (30, from chains.json),           |                                       star schema over gold:
     10 at a time, retries per item              | Auto Loader (new files only)            fact_price_daily,
       -> Lambda fetch_chain (container)         v                                         dim_product, dim_store,
          a) il-supermarket-scraper:      BRONZE price_items: one row per product          dim_chain, dim_date
             raw XML, all branches,              (file, chain, all values as text)         pages: price index,
             PriceFull/PromoFull/Stores          | field names matched, real types         Eilat check, fake promos,
          b) or kagglehub: that day's            v                                         shrinkflation,
             Kaggle file (geo-blocked)    SILVER stores, items (barcode master),          pipeline health
          -> S3 raw bucket (versioned)           price_obs, price_history (SCD2),
          -> manifest row                        promotion_obs, quarantine
  -> on a failed run: EventBridge rule           |
     -> SNS topic -> email                       v
                                          GOLD   price_index_daily (fixed basket), eilat_vs_center,
                                                 fake_promo_candidates, shrinkflation_candidates
One-off backfill: same Lambda, Map over
 sampled Kaggle versions (2024-09-22 on)
                                          OPS    run_log, dq_results, source_completeness
```

### Design decisions

- **Ingestion lives in AWS, not in Databricks.** The Tel Aviv region gives an Israeli IP for chains that block foreign traffic (tested 2026-11-03), and Free Edition compute runs in Ohio (`us-east-2`).
- **Databricks reads S3 directly; raw is not copied.** A read-only Unity Catalog external location (`supermarket_raw_scraper`, for `source=scraper/`) uses IAM role `supermarket-detective-databricks-read`. Bronze is written through a separate external location (`supermarket_bronze`, for `bronze/`), the only path that role can write to. Tested on Free Edition 2026-11-09: `LIST` and `read_files` work, and `.gz` is decompressed automatically. The storage credential and external location were created with the Databricks CLI (exact commands in the project log). The docs state a limited set of trusted outbound domains, but S3 was reachable in a real test, so re-test if it ever stops working. Auto Loader should use directory listing, because the role has no SQS/SNS permissions.
- **One Lambda function, one Map item per chain + file type.** 10 chains x 3 file types = 30 inputs to the same function, each with its own 15 minutes. A whole Shufersal day doesn't fit in one run; prices alone took 10.5 minutes (2026-11-08). A full chain also unzips to 2 GB or more, more than the function's 2 GB /tmp, so the handler zips, uploads and deletes each file as it arrives. A run killed at the 15-minute limit keeps what it already uploaded. Each item retries alone after 10 and 20 minutes, a failed item doesn't stop the others, and a final check marks the whole run failed if any item failed, which triggers the email alarm. `ok: true` means the files seen were saved, not that the list was complete: the scraper swallows listing timeouts, so silver needs a completeness check. The scraper's headless browser (Playwright) is only a fallback for reading the chain list from gov.il, not for downloading files; the container image includes it anyway. Fargate is the fallback if a chain ever needs longer than 15 minutes.
  - Considered and skipped: Lambda Managed Instances (90-minute timeout since 2026-09) run on EC2 capacity billed even when idle, so they cost more than this daily batch needs. Lambda durable functions (announced for Tel Aviv in 2026-04) could replace Step Functions as the orchestrator; Step Functions gives a visual run graph, which is better for a portfolio.
- **Two sources, one bronze.** Our own scraper keeps the raw XML (the real integration work); the Kaggle daily snapshot supplies history and the geo-blocked chains. Both land in the same raw layout tagged with `source`, and on days both cover a chain, a reconciliation check compares them.
- **S3 is the archive of record.** Raw files are kept forever in a versioned bucket; Databricks can be rebuilt from it.
- **`date=` in the S3 path is the collection date, not the file's date.** A run also collects older files still listed on a chain's site, so silver takes each file's date from its name.
- **Raw keeps files exactly as published; bronze keeps their values exactly as published.** Bronze has one row per product with every value as text, so it isn't a second copy of the same gzip bytes and it can be queried with SQL. Databricks' medallion guidance lists flattening under silver; it's done here on purpose, because the untouched files stay in raw. A parser bug never loses data: fix it and reload bronze from raw. Silver matches field names across chains and sets real types.
- **No config table for field names.** The 10 chains differ in only a few spellings (`ItemName` / `ItemNm`, `StoreID` / `StoreId`), so one `coalesce` per field handles them. A `chain_config` table was planned and dropped: it would be machinery with almost nothing to configure.
- **Price history is SCD Type 2,** keyed on chain + store + barcode: a new row when any tracked value changes, or when the product comes back after a file that arrived without it, with `first_seen` / `last_seen`. A missing file is not a missing product. Daily full files make this the "source sends a full dump" CDC problem.
- **Data quality at three levels:** file (expected chain/store/type arrived; row count within ±30% of the 7-day average), row (valid barcode, price > 0, parsable quantity; failures go to quarantine), business (price jump > 300% flagged).
- **Stay under the daily compute quota:** each silver run reads only bronze rows it hasn't seen. Silver keeps all stores of the 10 chains: the first load of 20 million rows took about a minute and 328 MB. The Kaggle backfill stays sampled.
- **Everything as code:** AWS resources in Terraform or CDK, Databricks pipelines and jobs in a bundle, all in GitHub with CI. Free Edition accounts can be deleted after long inactivity, so the repo must rebuild everything.

### Bronze table: `workspace.bronze.price_items`

External Delta table in `s3://supermarket-detective-raw-971422709739/bronze/price_items`, created by `databricks/notebooks/bronze_price_items.py`. One row per product in a PriceFull file, for all 10 chains. Loaded by Auto Loader in batches of 100 files, split into 20 partitions so that only about 5 files' parsed products sit in Python's memory at once. Job `bronze_load` runs this, the stores load and the promotions load. The same file can sit under several `date=` folders, so per-file checks group by `file_path`, and silver removes the repeats.

| Column | Type | Meaning |
|---|---|---|
| `file_path` | STRING | Full S3 path of the source file |
| `file_name` | STRING | File name; silver takes the file's date from it |
| `source` | STRING | `scraper` or `kaggle` |
| `chain` | STRING | Chain, from the S3 folder |
| `collected_date` | DATE | Collection date, from the S3 folder |
| `modified_at` | TIMESTAMP | When the file landed in S3 |
| `declared_count` | STRING | `Count` attribute of `<Items>`; found only in Keshet's sample, otherwise NULL |
| `tags_in_file` | INT | Number of `<Item>` tags counted in the decompressed file text; used for the per-file row check; counted after converting UTF-16 files to UTF-8 |
| `parse_error` | STRING | NULL normally. If the file can't be read, the error text, in one row with NULL `fields` |
| `header` | MAP<STRING, STRING> | The file's top-level fields, such as `StoreId` |
| `fields` | MAP<STRING, STRING> | This product's fields, as published |
| `loaded_at` | TIMESTAMP | When the row was loaded |

### Bronze table: `workspace.bronze.stores`

External Delta table in `s3://supermarket-detective-raw-971422709739/bronze/stores`, created by `databricks/notebooks/bronze_stores.py`. One row per store in a Stores or StoresFull file. It has the same columns as `price_items` without `declared_count`, with these differences in meaning:

| Column | Type | Meaning |
|---|---|---|
| `tags_in_file` | INT | Number of `<Store>` tags in the decompressed file text |
| `header` | MAP<STRING, STRING> | Everything above the `Store` tag: the file's top-level fields and the sub-chain (`SubChainID` or `SubChainId`, by chain) |
| `fields` | MAP<STRING, STRING> | This store's fields, as published |

### Bronze table: `workspace.bronze.promotion_items`

External Delta table in `s3://supermarket-detective-raw-971422709739/bronze/promotion_items`, created by `databricks/notebooks/bronze_promotion_items.py` (job `bronze_load`, task `load_promotions`). One row per product in a promotion in a PromoFull file, for all 10 chains. A promotion with no products, or a file with no promotions, stays as one row with NULL `item`. Loaded by Auto Loader in the same batches as `price_items`.

Promotion files come in two layouts, sometimes within one chain. Bronze keeps both as published, and silver matches them:

| Layout | Path to the product | Where the discount is |
|---|---|---|
| Groups | `Promotions/Promotion/Groups/Group/PromotionItems/PromotionItem` | On each product; `item` also holds the group's fields, such as `GroupID` |
| Flat | `Promotions/Promotion/PromotionItems/Item` | On the promotion; `item` holds only the code, type and gift flag |

| Column | Type | Meaning |
|---|---|---|
| `file_path` | STRING | Full S3 path of the source file |
| `file_name` | STRING | File name; silver takes the file's date from it |
| `source` | STRING | `scraper` or `kaggle` |
| `chain` | STRING | Chain, from the S3 folder |
| `collected_date` | DATE | Collection date, from the S3 folder |
| `modified_at` | TIMESTAMP | When the file landed in S3 |
| `declared_count` | STRING | `Count` attribute of `<Promotions>`, when there is one |
| `promotions_in_file` | INT | Number of `<Promotion>` tags in the decompressed file text |
| `items_in_file` | INT | Number of `<PromotionItem>` and `<Item>` tags in the decompressed file text |
| `parse_error` | STRING | NULL normally. If the file can't be read, the error text, in one row with NULL `promotion` and `item` |
| `header` | MAP<STRING, STRING> | The file's top-level fields, such as `StoreId` |
| `promotion_seq` | INT | The promotion's position in the file, from 1; with `file_path` it identifies one promotion |
| `promotion` | MAP<STRING, STRING> | The promotion's fields at any depth, except its products. A tag that repeats keeps its values joined by `\|` |
| `item` | MAP<STRING, STRING> | This product's fields, as published |
| `loaded_at` | TIMESTAMP | When the row was loaded |

Checked 2026-11-10: all 3,156 promotion files in S3 are loaded, 35,694,531 rows and 5,000,651 promotions, from all 10 chains. No file failed to parse, and every file's product rows and promotions match its tag counts. Two files contain no promotions, so each is one row with NULL `promotion`. The load took about 12 minutes.

### Silver table: `workspace.silver.stores`

Managed Delta table, rebuilt in full on each run by `databricks/notebooks/silver_stores.py` (job `silver_load`, not scheduled yet). One row per `chain` + `store_id`: the store's newest version in `bronze.stores`, by the date in the file name and then `modified_at`. Older versions of a store aren't kept here; they stay in bronze.

| Column | Type | Meaning |
|---|---|---|
| `chain` | STRING | Chain, from the S3 folder |
| `chain_id` | STRING | Chain ID published in the file |
| `sub_chain_id` | STRING | Sub-chain ID published in the file |
| `store_id` | STRING | Store ID without leading zeros, so it matches price files (`001` = `1`) |
| `store_id_raw` | STRING | Store ID as published |
| `store_name` | STRING | Store name as published |
| `address` | STRING | Address as published |
| `city_code` | INT | `City` when it is a non-zero number, otherwise NULL |
| `city_name` | STRING | `City` when it is Hebrew text, otherwise NULL |
| `zip_code` | STRING | ZIP as published; NULL if empty or all zeros |
| `store_type` | STRING | `StoreType` as published |
| `is_physical` | BOOLEAN | `store_type` is `1`; never NULL |
| `is_eilat` | BOOLEAN | Physical store in Eilat, by the Eilat rule below; never NULL |
| `last_file_date` | DATE | Date of the file this row came from |
| `source_file` | STRING | Full S3 path of that file |

Checked 2026-11-10: 974 stores, 945 physical, 13 in Eilat, no repeated key. The key needs no sub-chain: adding `sub_chain_id` changes no store count in any of the 10 chains, and no store ID appears twice in a file. 88 stores have neither a city code nor a city name, mostly Yohananof and Keshet.

Assumptions, not official definitions:

- **`StoreType` 1 means a physical store.** It excludes every online store found so far, but the codes' official meaning isn't confirmed.
- **The Eilat rule** (`City` or a ZIP starting with `88`) is ours. Its 13 stores match the count made separately from the chains' files.
- **`city_code` looks like the Central Bureau of Statistics locality codes.** Only `2600` is confirmed, and there is no code-to-name lookup yet.

### Silver table: `workspace.silver.price_obs`

Managed Delta table, clustered by `file_date` and `chain`, built by `databricks/notebooks/silver_price_obs.py` (job `silver_load`, task `build_price_obs`, which runs after `build_stores`; not scheduled yet). One row per `chain` + `store_id` + `item_code` + `file_date`: what the chain published for that product on that day. It is an observation, not proof of the shelf price. Product names are not here; they go in `items`, so they aren't repeated about 7.4 million times a day.

**Loading:** each run reads only bronze rows newer than the table's latest `bronze_loaded_at`, and saves them with `MERGE` on the key above. An existing row is replaced only by a later file from the same source, or by a scraper row replacing a Kaggle row. When one load has several candidates for a key, the scraper wins, then the latest file. To rebuild, drop the table and run the job. If `quarantine` is empty, the run reads all of bronze to fill it, and rows already in `price_obs` don't change.

**Rejected rows go to `silver.quarantine`:** files that failed to parse; a chain ID that differs from the chain's ID in `silver.stores`; no file date, store ID or barcode; a price that isn't a positive number. The same notebook decides both outcomes, and saves rejects first, because saving prices moves the watermark. Prices from stores missing from the Stores file are kept.

| Column | Type | Meaning |
|---|---|---|
| `chain` | STRING | Chain, from the S3 folder |
| `source` | STRING | `scraper` or `kaggle` |
| `chain_id` | STRING | Chain ID published in the file |
| `sub_chain_id` | STRING | Sub-chain ID published in the file |
| `store_id` | STRING | Store ID without leading zeros; joins to `silver.stores` with `chain` |
| `item_code` | STRING | Product code as published. Not always comparable across chains |
| `item_type` | STRING | `ItemType` as published |
| `price` | DECIMAL(10,2) | Regular price, always above 0 |
| `unit_price` | DECIMAL(12,4) | `UnitOfMeasurePrice`; NULL if not a number |
| `quantity` | DECIMAL(12,3) | Package size; NULL if not a number |
| `unit_qty` | STRING | `UnitQty` as published |
| `unit_of_measure` | STRING | `UnitOfMeasure` as published; spellings vary |
| `qty_in_package` | DECIMAL(12,3) | `QtyInPackage`; NULL if not a number, as for "unknown" |
| `is_weighted` | BOOLEAN | `bIsWeighted` is `1` |
| `allow_discount` | BOOLEAN | `AllowDiscount` is `1` |
| `price_updated_at` | TIMESTAMP | The chain's own last price update, as published (`PriceUpdateDate` or `PriceUpdateTime`) |
| `file_date` | DATE | Date in the file name: the day the data represents |
| `file_time` | STRING | Date and time in the file name, as 14 digits; picks the latest file |
| `source_file` | STRING | Full S3 path of the file |
| `collected_date` | DATE | Date of the S3 folder: when the file was collected |
| `modified_at` | TIMESTAMP | When the file landed in S3 |
| `bronze_loaded_at` | TIMESTAMP | When the row was loaded into bronze; the watermark for the next run |

Checked 2026-11-10: of 24,784,723 bronze rows, 20,029,515 kept, 4,748,410 repeats dropped, 6,798 rejected (2,068 with a price of zero or less, 4,730 from one Carrefour file in Keshet's folder), which adds up to the bronze total. 971 chain + store pairs, 53 file dates, no repeated key. A second run inserted 0 rows and updated 0. 67 stores publish prices but aren't in `silver.stores` (Carrefour 55, Shufersal 9, Keshet 2, Yohananof 1); all 13 Eilat stores have prices.

### Silver table: `workspace.silver.quarantine`

Managed Delta table, written by `silver_price_obs.py`. One row per rejected bronze row, so a file sitting under two `date=` folders appears twice. A re-run adds nothing, because `row_key` already exists.

| Column | Type | Meaning |
|---|---|---|
| `row_key` | STRING | SHA-256 of the file path, the row's values and any parse error |
| `reject_reason` | STRING | `file_not_readable`, `wrong_chain`, `no_file_date`, `no_store_id`, `no_item_code`, `price_not_a_number` or `price_zero_or_less` |
| `chain` | STRING | Chain, from the S3 folder |
| `source` | STRING | `scraper` or `kaggle` |
| `chain_id` | STRING | Chain ID published in the file |
| `store_id` | STRING | Store ID without leading zeros |
| `item_code` | STRING | Product code as published |
| `price_raw` | STRING | `ItemPrice` as published |
| `parse_error` | STRING | Error text if the file couldn't be read |
| `file_date` | DATE | Date in the file name |
| `source_file` | STRING | Full S3 path of the file |
| `collected_date` | DATE | Date of the S3 folder |
| `modified_at` | TIMESTAMP | When the file landed in S3 |
| `bronze_loaded_at` | TIMESTAMP | When the row was loaded into bronze |
| `header` | MAP<STRING, STRING> | The file's top-level fields, as published |
| `fields` | MAP<STRING, STRING> | This row's fields, as published |
| `quarantined_at` | TIMESTAMP | When the row was saved here |

Checked 2026-11-10: 6,798 rows (2,068 `price_zero_or_less`, 4,730 `wrong_chain`), which with kept rows and repeats adds up to bronze.

### Silver table: `workspace.silver.promotion_obs`

Managed Delta table, clustered by `file_date` and `chain`, built by `databricks/notebooks/silver_promotion_obs.py` (job `silver_load`, task `build_promotion_obs`, which runs after `build_stores`; not scheduled yet). One row per `chain` + `store_id` + `promotion_id` + `group_id` + `item_code` + `file_date`: what the chain published about that product in that promotion on that day. It is an observation, and it says nothing about whether the deal is really cheaper; gold answers that by comparing with `price_obs`.

**Loading:** the same as `price_obs`: only bronze rows newer than the table's latest `bronze_loaded_at`, saved with `MERGE` on the key above. An existing row is replaced only by a later file from the same source, or by a scraper row replacing a Kaggle row. To rebuild, drop the table and run the job.

**Matching the two layouts:** `promotion_id` loses its leading zeros, because Keshet publishes the same promotion as `1145634` in one layout and `0001145634` in the other. The flat layout has no groups, so its `group_id` is `1`. Reward type, quantities, price and discount come from the product in the Groups layout and from the promotion in the flat layout.

**Repeats inside one file:** a product can appear twice in the same group of a promotion, usually once at 0.00 and once at its real price (Shufersal, 183,548 of its 183,652 repeats). The row with a positive price is kept. If both are positive, the lower price is kept (98 cases). The official meaning of these pairs isn't confirmed.

**Rejected, stored in `silver.promotion_quarantine`:** a file that failed to parse, a file with no promotions, a chain ID that differs from the chain's ID in `silver.stores`, no file date, store ID, promotion ID or barcode, or a negative price. It has the columns of `silver.quarantine` plus `promotion_id`, with `promotion` and `item` in place of `fields`, and it is separate because the price checks in `silver_dq` count the rows of `silver.quarantine`. A price of 0 or a missing price isn't rejected: many promotions give only a discount percent or a gift.

| Column | Type | Meaning |
|---|---|---|
| `chain` | STRING | Chain, from the S3 folder |
| `source` | STRING | `scraper` or `kaggle` |
| `chain_id` | STRING | Chain ID published in the file |
| `sub_chain_id` | STRING | Sub-chain ID published in the file |
| `store_id` | STRING | Store ID without leading zeros; joins to `silver.stores` with `chain` |
| `promotion_id` | STRING | Promotion ID without leading zeros; unique only within a chain |
| `group_id` | STRING | `GroupID`, or `1` in the flat layout |
| `item_code` | STRING | Product code as published; joins to `price_obs` with `chain` and `store_id` |
| `item_type` | STRING | `ItemType` as published |
| `layout` | STRING | `groups` or `flat` |
| `description` | STRING | Promotion text as published |
| `start_at` | TIMESTAMP | Start of the promotion; from one field, or from date + hour in the flat layout |
| `end_at` | TIMESTAMP | End of the promotion, built the same way |
| `promotion_updated_at` | TIMESTAMP | The chain's own last update of the promotion |
| `reward_type` | STRING | `RewardType` as published; official meaning of the codes not confirmed |
| `min_qty` | DECIMAL(12,3) | Quantity needed for the deal; NULL if not a number |
| `max_qty` | DECIMAL(12,3) | Maximum quantity; NULL if not a number or not set |
| `discounted_price` | DECIMAL(12,2) | Deal price as published, usually for `min_qty` units together, not per unit. 0 doesn't mean free |
| `discounted_price_per_unit` | DECIMAL(14,4) | `DiscountedPricePerMida`; unreliable in some chains (3 for 14 shows 14) |
| `discount_rate` | DECIMAL(12,4) | Discount percent as published |
| `discount_type` | STRING | `DiscountType` as published |
| `min_purchase_amount` | DECIMAL(12,2) | Minimum purchase; NULL if not a number or not set |
| `is_weighted` | BOOLEAN | `bIsWeighted`, or `IsWeightedPromo` in the flat layout, is `1` |
| `is_gift_item` | BOOLEAN | The product's `IsGiftItem` is `1`; NULL in the Groups layout, whose promotion-level flag holds values like 3 or 8 |
| `allow_multiple_discounts` | BOOLEAN | `AllowMultipleDiscounts` is `1` |
| `is_coupon` | BOOLEAN | `AdditionalIsCoupon` is `1` |
| `club_code` | STRING | First number in the club field. `0` looks like "all customers", but it isn't official, and Carrefour lists named clubs under `0` |
| `club_raw` | STRING | Club field as published |
| `file_date` | DATE | Date in the file name: the day the data represents |
| `file_time` | STRING | Date and time in the file name, as 14 digits; picks the latest file |
| `source_file` | STRING | Full S3 path of the file |
| `collected_date` | DATE | Date of the S3 folder: when the file was collected |
| `modified_at` | TIMESTAMP | When the file landed in S3 |
| `bronze_loaded_at` | TIMESTAMP | When the row was loaded into bronze; the watermark for the next run |

Other promotion fields, such as `Remarks` and `PromotionDays`, stay in bronze.

Checked 2026-11-10: of 35,694,531 bronze rows, 31,261,575 kept, 4,432,950 repeats dropped, 6 rejected (2 files with no promotions, 4 negative prices), which adds up to the bronze total. No repeated key. 10 chains, 981 chain + store pairs, 44 file dates, of which 3 are full days; the others come from old files still listed on the sites. A full day is about 11.5 million rows, and the table is 481 MB. The first load took under 3 minutes; a second run read 0 rows and inserted 0. 14 stores publish promotions but aren't in `silver.stores` (Shufersal 13, Keshet 1). 104 rows had already expired on their file date. `silver_dq` has no promotion checks yet.

### Silver table: `workspace.silver.price_history`

Managed Delta table, clustered by `chain`, `store_id` and `item_code`, built by `databricks/notebooks/silver_price_history.py` (job `silver_load`, task `build_price_history`, which runs after `build_price_obs`; not scheduled yet). SCD Type 2: one row per period in which a product at a store kept the same values. This is the long-term price table.

**When a new period starts** (`start_reason`):
- `first_seen`: the product's first file at this store.
- `returned`: a file of this store arrived without the product, and a later one has it again. The store's days with a file are numbered in order (`dense_rank`), so a day with no file at all leaves no gap and doesn't split a period.
- `value_changed`: any tracked value differs from the previous day it was seen: price, unit price, quantity, unit quantity, unit of measure, quantity in package, weighed, allow discount or item type. The comparison treats two NULLs as equal.

**Loading:** rebuilt in full from `price_obs` on each run (`CREATE OR REPLACE`), so late or backfilled files need no special handling. This works only while `price_obs` keeps every day. When `price_obs` is trimmed to about 90 days after the Kaggle backfill, this table must switch to an incremental `MERGE`.

| Column | Type | Meaning |
|---|---|---|
| `chain` | STRING | Chain |
| `store_id` | STRING | Store ID without leading zeros; joins to `silver.stores` with `chain` |
| `item_code` | STRING | Product code as published |
| `period_no` | BIGINT | 1, 2, 3… per chain + store + product; with those, the key |
| `item_type` | STRING | `ItemType` during the period |
| `price` | DECIMAL(10,2) | Shelf price during the period |
| `unit_price` | DECIMAL(12,4) | Price per unit of measure during the period |
| `quantity`, `unit_qty`, `unit_of_measure`, `qty_in_package` | as in `price_obs` | Package values during the period |
| `is_weighted`, `allow_discount` | BOOLEAN | Flags during the period |
| `first_seen` | DATE | First file date in the period |
| `last_seen` | DATE | Last file date in the period |
| `days_observed` | BIGINT | Files of this store that had the product in the period |
| `start_reason` | STRING | `first_seen`, `returned` or `value_changed` |
| `is_current` | BOOLEAN | The period includes the store's latest file |
| `first_source_file`, `last_source_file` | STRING | S3 paths of the first and last file of the period |

Checked 2026-11-10: 20,029,515 `price_obs` rows became 7,643,904 periods (7,521,408 first seen, 119,555 value changed, 2,941 returned), 7,489,021 of them current, 74 MB, built in 38 s. The ratio is low because only a few full days exist and every product needs a first row. 23,621 of the value changes kept the same price, mostly `allow_discount` flips (20,259), so analyses of price changes should compare `price`, not count periods.

### Silver table: `workspace.silver.items`

Managed Delta table, clustered by `item_key`, built by `databricks/notebooks/silver_items.py` (job `silver_load`, task `build_items`, which runs after `build_stores`; not scheduled yet). One row per product, with the descriptive fields that `price_obs` does not carry: name, manufacturer, country, package size. A product is only here if one of its rows reached `price_obs`: the notebook applies the same reject rules as `silver_price_obs.py` (file not readable, wrong chain, no file date, store or item code, price not a positive number).

**Key rule.** A 13-digit barcode is the same product in every chain, but some codes are a chain's own internal numbers (a code like `374` is a mushroom in one chain and a fish in another). A code is `shared` when `ItemType` is `1` or the code looks like a barcode (8, 12 or 13 digits). Every other code is `chain`: its key is `chain:item_code`. The official meaning of `ItemType` is not confirmed. The data says: `1` means a barcode for most rows, `0` means an internal code for most rows, and Mahsani Ashuk and Victory publish real barcodes as `0`. That is why the barcode shape is part of the rule. Measured 2026-11-11 on all of `price_obs` (246,583 chain + code pairs):
- Rule on `ItemType` alone would put 1,679 chain + code pairs (1,410 real barcodes, 1.3% of all keys) in the wrong scope. Example: Nutella 750 g (`80176800`) is `1` in seven chains and `0` in Mahsani Ashuk and Victory. Adding the barcode shape removes this.
- `ItemType` 0 codes that appear in 2+ chains (1,355) are mostly short internal codes with different products, so they stay chain-scoped. Barcode-shaped `0` codes that appear in 2+ chains and are never `1` (17 codes) are the same product in the sampled names.
- `ItemType` 1 codes shorter than 8 digits: 660 codes, every one in a single chain, so no false merge.
- `ItemType` changes over time within one chain for 13 pairs (0.005%). If the new type moves the code to another scope, the product gets a second row. Not handled.

**Loading:** incremental. The notebook reads only bronze rows newer than the largest `bronze_loaded_at` in `items`, builds one row per key from them, joins that to the stored row and MERGEs on `item_key`. Attributes come from the latest observation (file date, then file time, then load time) and change only if the new observation is not older than the stored one. `first_seen` and `last_seen` only widen; `chains` and `names` only grow (distinct, sorted). If the table is empty, it is built from all of bronze. A second run reads 0 rows and changes 0.

| Column | Type | Meaning |
|---|---|---|
| `item_key` | STRING | Key: `item_code` when `shared`, `chain:item_code` when `chain` |
| `item_scope` | STRING | `shared` or `chain` |
| `chain` | STRING | Chain; NULL when `shared` |
| `item_code` | STRING | Product code as published |
| `item_type` | STRING | `ItemType` of the latest observation |
| `item_name` | STRING | Name; `ItemName` or `ItemNm` depending on the chain |
| `manufacturer_name` | STRING | `ManufacturerName` or `ManufactureName`, depending on the chain |
| `manufacture_country` | STRING | `ManufactureCountry` |
| `manufacturer_item_description` | STRING | `ManufacturerItemDescription` or `ManufactureItemDescription` |
| `quantity` | DECIMAL(12,3) | Package quantity |
| `unit_qty`, `unit_of_measure` | STRING | As in `price_obs` |
| `is_weighted` | BOOLEAN | Sold by weight |
| `chains`, `chains_count` | ARRAY<STRING>, INT | Chains that published the product, and how many |
| `names`, `name_variants` | ARRAY<STRING>, INT | Distinct names seen, and how many |
| `first_seen`, `last_seen` | DATE | First and last file date |
| `last_file_time` | STRING | File time of the latest observation (used for the "not older" test) |
| `last_source_file` | STRING | S3 path of the latest file |
| `bronze_loaded_at` | TIMESTAMP | Watermark: largest bronze load time seen |

Checked 2026-11-11: 126,809 items, 114,101 `shared` (36,860 seen in 2+ chains) and 12,708 `chain`. All 246,583 chain + code pairs in `price_obs` map to an item and no item is unused. No item without a name; 29,471 without a manufacturer name and 34,245 without a country. The most widely published products (10 chains) have up to 16 different names. 9.3 MB in 4 files. First build 53 s, second run 17 s with 0 rows read, inserted or updated (Delta history, `numSourceRows` 0). Databricks Free Edition, serverless.

### Silver table: `workspace.silver.dq_results`

Managed Delta table, written by `databricks/notebooks/silver_dq.py` (job `silver_load`, task `build_dq`, which runs after `build_price_obs`, `build_price_history` and `build_items`). One row per check per run, appended. Each check is one query that returns one number: above `fail_above` is FAIL, above `warn_above` is WARNING, otherwise PASS. Results are saved first, then any FAIL stops the job.

| Column | Type | Meaning |
|---|---|---|
| `run_at` | TIMESTAMP | When the run started; the same for all its checks |
| `check_name` | STRING | Name of the check |
| `check_level` | STRING | `row`, `file` or `business` |
| `file_date` | DATE | The latest date in `price_obs` when the check ran |
| `observed` | DOUBLE | The number the check returned |
| `warn_above` | DOUBLE | WARNING above this; NULL if none |
| `fail_above` | DOUBLE | FAIL above this; NULL if none |
| `outcome` | STRING | `PASS`, `WARNING` or `FAIL` |
| `meaning` | STRING | What the check looks for |

| Check | Level | WARNING above | FAIL above | 2026-11-10 |
|---|---|---|---|---|
| `repeated_keys` | row | - | 0 | 0, PASS |
| `bad_prices` | row | - | 0 | 0, PASS |
| `missing_keys` | row | - | 0 | 0, PASS |
| `reject_percent` | row | 0.1 | 5 | 0.006, PASS |
| `files_not_readable` | file | 0 | - | 0, PASS |
| `wrong_chain_rows` | file | 0 | - | 2,365, WARNING |
| `stores_without_record` | business | 0 | - | 56, WARNING |
| `eilat_stores_missing` | business | 0 | - | 0, PASS |
| `price_jump_over_300pct` | business | 100 | - | 7, PASS |
| `history_days_mismatch` | row | - | 0 | 0, PASS |
| `history_overlapping_periods` | row | - | 0 | 0, PASS |
| `items_repeated_keys` | row | - | 0 | 0, PASS |
| `price_obs_without_item` | row | - | 0 | 0, PASS |
| `items_without_name` | row | 0 | - | 0, PASS |

`price_obs_without_item` looks at the latest day only. The limit of 0 on items without a name is measured (0 today), so any missing name shows as a WARNING. The limit of 100 on price jumps comes from one measured day and is provisional. The stop-on-FAIL path has not been seen in a real run yet. The file-level checks for expected stores and row counts come with the completeness work in S6.

## Stores files: what the data looks like

Checked on the latest Stores file of each of the 10 chains, collected 2026-11-09. Bronze keeps these values as published; silver handles them.

| `City` holds | Chains | Silver rule |
|---|---|---|
| Numeric code (`2600` = Eilat) | Shufersal, Rami Levy, Carrefour, Osher Ad, Tiv Taam, Hazi Hinam | Look up the code; the numbers look like the Central Bureau of Statistics locality codes (only `2600` confirmed so far) |
| City name in Hebrew | Victory, Mahsani Ashuk | Use as is |
| `0` | Yohananof (51 of 51), Keshet (26 of 28), Rami Levy (3) | Use another column, see below |

- **ZIP code:** usable for Keshet, but all Yohananof ZIPs are `0000000` or empty. Yohananof's city can come only from the store name or a hand-made store-to-city table.
- **Eilat rule:** `City` is `2600` or `אילת`, or the ZIP starts with `88`, and the store is physical. Don't search for the word `אילת` in names or addresses: it matches streets and stores in other cities.
- **Eilat stores:** 13 physical stores in 4 chains (Shufersal 7, Carrefour 3, Mahsani Ashuk 2, Rami Levy 1). One Carrefour Eilat store has a wrong ZIP (`4333345`), so the ZIP alone isn't enough. Mahsani Ashuk's `96 אינטרנט אילת` is online and is excluded.
- **No Eilat branch at Keshet or Yohananof:** confirmed against each chain's own branch list on 2026-11-10 (Keshet 19 branches + 2 robotic delivery centres, Yohananof 47 branches).
- **Not physical stores:** online stores at Mahsani Ashuk (96, 97), Victory (97) and Hazi Hinam (103), and probably Keshet's `ממ"ר` stores (116, 120), which look like delivery centres. `silver.stores` marks these with `is_physical`.
- **In the feed but not on the chain's branch list:** Yohananof 002 (`יוחננוף ישן`) and Keshet 010 (`גבעת רם`); check in silver whether they still publish prices. Yohananof 146, 150 and 152 are pickup points.
- **Format:** every chain uses a `Store` tag, at different depths (Keshet: `Root/SubChains/SubChain/Stores/Store`). Six chains are UTF-16. Shufersal's root tag is `Chain`. Hazi Hinam's files are named `StoresFull...gz.xml.gz` but are gzipped only once.

## The Kaggle dataset

https://www.kaggle.com/datasets/erlichsefi/israeli-supermarkets-2024 (CC BY-NC-SA 3.0 IGO, by Sefi Erlich)

- One Kaggle version per day, each a full snapshot of that day: versions 6–711 cover 2024-09-22 to 2026-11-03. Public files download without a Kaggle login (`kagglehub`), one file at a time, from any version.
- 358 files, 13.9 GB per version. One folder per chain (34 chains, e.g. `shufersal/`, `ramilevy/`, `yohananof/`, `yaynotbitanandcarrefour/`), each with `price_full_file_<chain>.csv`, `price_file_`, `promo_full_file_`, `promo_file_`, `store_file_` (plus JSON status files), and dataset-level `index.json` (version → date), `scraper_quality.json`, `parser_quality.json`.
- Columns are the regulation's field names, lowercased. Prices: `chainid, subchainid, storeid, itemcode, itemname, manufacturename, quantity, unitqty, unitofmeasure, bisweighted, itemprice, unitofmeasureprice, allowdiscount, itemstatus, priceupdatetime, lastsaledatetime` + `file_name`. Promos: `promotionid, promotiondescription, promotionstartdatetime, promotionenddatetime, minnoofitemoffered, clubid, groups` (items and discounted prices as nested JSON). Stores: `storeid, storename, address, city, storetype`.
- Quirks found in a sample (Hazi Hinam, 2026-11-02; 163,176 price rows from 13 stores):
  - `file_name`, `chainid`, `storeid` are filled only on the first row of each source file (26 of 163,176 rows). The store of every other row depends on row order, so it must be forward-filled carefully, or read from our own raw XML instead.
  - Empty values are the literal string `''`; nested JSON uses `NO_BODY` placeholders.
  - 13-digit chain ids were read as floats (`7290700100008.0`); read ids as strings.
  - `city` is sometimes a name and sometimes a CBS settlement code (`8300` = Rishon LeZion).
  - Some rows have no `itemprice`, and some have no quantity or unit.

## Chains blocked outside Israel

As of 2026-11-02, 26 of 33 chains can be downloaded from outside Israel (Shufersal, Rami Levy, Yohananof, Tiv Taam, Osher Ad, Yellow…). Seven are geo-blocked: Hazi Hinam, Victory, Mahsani Ashuk, Yeinot Bitan/Carrefour, Super-Pharm, Netiv Hased, Het Cohen. Source: https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers/blob/reports/reachability/reachability.md
**Tested 2026-11-03 from a Lambda in il-central-1** (public IP 16.164.187.95, which geolocates to Tel Aviv, IL, on Amazon's network): all seven blocked chains, plus Shufersal and Rami Levy as controls, returned a store file. So the daily run can scrape every chain itself; today's Kaggle file is only a fallback for a chain whose site fails. Test code: `ingestion/reach_test/` (the Lambda, image and role were deleted afterwards). One run of small store files; price files come from the same sites, but stability over several days is still unproven.

Library notes from the test (il-supermarket-scraper 1.0.19): scraper classes take `file_output=DiskFileOutput(storage_path=...)`, not `folder_name` (the engine docstring is out of date), and `scrape()` is an async generator that yields each downloaded file.

## Licenses

- `il-supermarket-scraper` (https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers): custom license, non-commercial use only, credit Sefi Erlich and link the repo.
- Kaggle dataset: CC BY-NC-SA 3.0 IGO (non-commercial, attribution, share-alike).
- Regulation: https://www.gov.il/he/departments/legalInfo/cpfta_prices_regulations

## Accounts

- AWS: region `il-central-1` (Tel Aviv).
- Databricks Free Edition workspace: https://dbc-ac0d914f-528b.cloud.databricks.com (workspace id 7474647427258058).

## Folders

- `ingestion/` - Lambda container (scraper + Kaggle pull + upload), backfill mode, tests
- `infra/` - AWS resources as code (S3, ECR, Lambda, Step Functions, EventBridge schedule, IAM, alarms)
- `databricks/` - bundle: notebooks (bronze, silver) and jobs
- `powerbi/` - .pbix and screenshots for X posts
- `data/` - local scratch only (git-ignored)

## Plan

| # | Milestone | What exists at the end | Target |
|---|---|---|---|
| 0 | Prove the risky parts | AWS Tel Aviv reaches every chain, Databricks accepts uploads, Power BI connects to the Free Edition warehouse | Done 2026-11-03 |
| 1 | Daily collection in the cloud | Every morning at 09:00 AWS downloads all chains' files into S3, where Databricks can read them directly, with no laptop involved | ~2026-11-10 |
| 2 | Bronze + silver | Raw files become clean tables (stores, products, prices) with SCD2 price history, quality checks, quarantine and ops tables, for all 10 chains | ~2026-11-17 |
| 3 | History backfill | Kaggle snapshots (weekly back to 2024-09-22, daily for the last 60 days and around Hanukkah 2025) in the same S3 bucket under `source=kaggle/`, sampled stores only, through the same bronze/silver | ~2026-11-21 |
| 4 | Gold + Power BI v1 | Price index and the Eilat VAT check; first X post | ~2026-11-28 |
| 5 | Hanukkah index | Holiday prices 2026 vs. Hanukkah 2025 (2025-12-14 to 12-22) | before 2026-12-04 |
| 6 | Extras | Fake promotions, shrinkflation, pipeline health page, n8n + LLM alert to Telegram | December |

Why daily collection comes before history: the chain sites keep only recent files, so every day without the daily run is lost raw data, while Kaggle's archive stays online. The backfill reuses the daily path (same bucket, layout and loading), and which stores to backfill is decided while building silver.

Milestone 1 is built in eight pieces, each working before the next: (1) Terraform setup + S3 raw bucket, (2) ECR repository, (3) `fetch_chain` Python code, (4) Lambda + IAM role, (5) Step Functions Map over the chain list, (6) EventBridge Scheduler at 09:00 (first 07:00, moved because the scraper disables two chains until 08:00), (7) Databricks read access to S3 (first planned as a push to a volume), (8) failure alarm by email.

Milestone 0 results (2026-11-03): all seven geo-blocked chains answered a Lambda in il-central-1. A 32.8 MB Kaggle price file uploaded to the volume in 7 s and read with SQL; the Kaggle CSVs need `multiLine => true, escape => '"'`, otherwise line breaks inside product names split rows (163,660 rows and 41 "stores" instead of 163,176 and 13), which becomes the first reconciliation check. Power BI Desktop imported a test table from the warehouse (Databricks connector, Import mode).
