# Supermarket Detective

Israeli supermarket prices as a lakehouse: every chain's legally required price and promo files, kept as full price history, with a Power BI report on top.
Questions it answers: is Eilat really 15.3% cheaper (no VAT)? Which "promotions" are not cheaper than the regular price was? Which products shrank while the price stayed? How do prices move before holidays?

Stack: AWS (Tel Aviv region, ingestion) → Databricks Free Edition (Unity Catalog, Auto Loader, notebooks with SQL and MERGE run as Jobs, bundles) → Power BI. Everything scheduled runs in the cloud; the laptop is only for writing code and building the Power BI report.

## Architecture

```
AWS il-central-1 (Tel Aviv)               DATABRICKS FREE EDITION                        POWER BI
---------------------------               ------------------------------------------     ---------------
EventBridge Scheduler (Sun-Fri 09:00)     External locations raw_scraper (Tel Aviv)    Desktop (authoring),
  -> Step Functions state machine           + raw_kaggle_ohio read S3, no copy:          Import from the SQL
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
  -> WriteMarker: one JSON file in               |
     source=scraper/_markers/ per run  ----file arrival trigger----> job daily_load
  -> on a failed run: EventBridge rule           |
     -> SNS topic -> email                       v
                                          GOLD   price_index_daily (fixed basket), eilat_vs_center,
                                                 fake_promo_candidates, shrinkflation_candidates
One-off backfill: Lambda fetch_kaggle, Map over
 the files of a slice of Kaggle versions, by hand
                                          OPS    run_log, dq_results, source_completeness
```

### Design decisions

- **Ingestion lives in AWS, not in Databricks.** The Tel Aviv region gives an Israeli IP for chains that block foreign traffic (tested 2026-11-03), and Free Edition compute runs in Ohio (`us-east-2`).
- **Bronze is a managed Delta table in Ohio, next to the compute (changed 2026-11-12).** Databricks Free Edition runs in `us-east-2`. Bronze used to be an external table in the Tel Aviv bucket (`bronze/`), so every read and every write crossed the ocean: on the first Kaggle slice the cross-region networking was about 15 DBU against about 3 DBU of real compute. Rule now: whatever Databricks reads repeatedly lives in Databricks' own storage or in an Ohio bucket; only the scraper stays in Tel Aviv, because the chain sites need an Israeli IP. The three bronze tables were copied once with `CREATE TABLE ... DEEP CLONE` (same names, columns and Auto Loader checkpoints in S3, so no file loaded twice). The old external tables (`bronze.<name>_ta_external`) were dropped and their data folders under `s3://supermarket-detective-raw-971422709739/bronze/` deleted on 2026-11-16 (versioning plus the 30-day noncurrent rule gives a 30-day undo window). Raw stays in S3, so bronze can be rebuilt. `bronze/_checkpoints/` stays: the Auto Loader checkpoints of the managed tables live there, so the external location `supermarket_bronze` and the IAM write right on `bronze/*` are still in use. The Kaggle lane has its own raw bucket in Ohio, `supermarket-detective-kaggle-971422709739` (external location `supermarket_raw_kaggle_ohio`, read-only), so Kaggle files are not read across the ocean either.
- **Databricks reads S3 directly; raw is not copied.** Read-only Unity Catalog external locations (`supermarket_raw_scraper` for `source=scraper/`, `supermarket_raw_kaggle` for `source=kaggle/`, both on storage credential `supermarket_s3_read`) use IAM role `supermarket-detective-databricks-read`. Tested on Free Edition 2026-11-09: `LIST` and `read_files` work, and `.gz` is decompressed automatically. The storage credential and external location were created with the Databricks CLI (exact commands in the project log). The docs state a limited set of trusted outbound domains, but S3 was reachable in a real test, so re-test if it ever stops working. Auto Loader should use directory listing, because the role has no SQS/SNS permissions.
- **One Lambda function, one Map item per chain + file type.** 10 chains x 3 file types = 30 inputs to the same function, each with its own 15 minutes. A whole Shufersal day doesn't fit in one run; prices alone took 10.5 minutes (2026-11-08). A full chain also unzips to 2 GB or more, more than the function's 2 GB /tmp, so the handler zips, uploads and deletes each file as it arrives. A run killed at the 15-minute limit keeps what it already uploaded. Each item retries alone after 10 and 20 minutes, a failed item doesn't stop the others, and a final check marks the whole run failed if any item failed, which triggers the email alarm. `ok: true` means the files seen were saved, not that the list was complete: the scraper swallows listing timeouts, so silver needs a completeness check. The scraper's headless browser (Playwright) is only a fallback for reading the chain list from gov.il, not for downloading files; the container image includes it anyway. Fargate is the fallback if a chain ever needs longer than 15 minutes.
  - Considered and skipped: Lambda Managed Instances (90-minute timeout since 2026-09) run on EC2 capacity billed even when idle, so they cost more than this daily batch needs. Lambda durable functions (announced for Tel Aviv in 2026-04) could replace Step Functions as the orchestrator; Step Functions gives a visual run graph, which is better for a portfolio.
- **AWS starts Databricks by writing a marker file; no token lives in AWS.** When the Map ends, state `WriteMarker` puts `source=scraper/_markers/<YYYY-MM-DD>_<execution>.json` (execution id, start time, `some_failed`) into the raw bucket, and job `daily_load` (bronze_load, then silver_load) has a file arrival trigger on that folder (wait 60 s after the last change). The marker is written even when some chains failed; the run then ends FAILED and the alarm email fires. The key is new for every execution because a file arrival trigger fires on new files, not on overwrites. `_markers/` is its own flat folder because listing mode allows at most 10,000 files per watched path. The external location `supermarket_raw_scraper` has file events turned off, so Databricks finds new files by listing the folder: the read-only role has no SNS/SQS rights, and none were added. Decision record, 2026-11-11: EventBridge Connections are disabled in il-central-1, so the Step Functions HTTP task to the Jobs API cannot be used; a Lambda holding a Databricks token was rejected (the token expires every 90 days); a relay in another region was rejected as more moving parts. Trade-off: if the trigger does not fire, AWS does not see it; a missed day shows up in `ops.run_log` and the DQ checks.
- **Two sources, one bronze.** Our own scraper keeps the raw XML (the real integration work); the Kaggle snapshots supply history. Both land in the same raw layout tagged with `source` and in the same bronze and silver tables with `source` = `scraper` or `kaggle`. Where both hold the same source file, DQ check `kaggle_vs_scraper_file_mismatch` compares them; in silver the scraper row wins a tie. The Kaggle backfill is loaded in batches from a queue table by job `kaggle_backfill` (every 30 minutes, and it does nothing while `daily_load` runs); its plan is in [docs/backfill-plan.md](docs/backfill-plan.md).
- **S3 is the archive of record.** Raw files are kept forever in a versioned bucket; Databricks can be rebuilt from it.
- **`date=` in the S3 path is the collection date, not the file's date.** A run also collects older files still listed on a chain's site, so silver takes each file's date from its name.
- **Raw keeps files exactly as published; bronze keeps their values exactly as published.** Bronze has one row per product with every value as text, so it isn't a second copy of the same gzip bytes and it can be queried with SQL. Databricks' medallion guidance lists flattening under silver; it's done here on purpose, because the untouched files stay in raw. A parser bug never loses data: fix it and reload bronze from raw. Silver matches field names across chains and sets real types.
- **No config table for field names.** The 10 chains differ in only a few spellings (`ItemName` / `ItemNm`, `StoreID` / `StoreId`), so one `coalesce` per field handles them. A `chain_config` table was planned and dropped: it would be machinery with almost nothing to configure.
- **Price history is SCD Type 2,** keyed on chain + store + barcode: a new row when any tracked value changes, or when the product comes back after a file that arrived without it, with `first_seen` / `last_seen`. A missing file is not a missing product. Daily full files make this the "source sends a full dump" CDC problem.
- **Data quality at three levels:** file (expected chain/store/type arrived; row count within ±30% of the 7-day average), row (valid barcode, price > 0, parsable quantity; failures go to quarantine), business (price jump > 300% flagged).
- **Stay under the daily compute quota:** each silver run reads only bronze rows it hasn't seen. Silver keeps all stores of the 10 chains: the first load of 20 million rows took about a minute and 328 MB. The Kaggle backfill is sampled in time (weekly, plus daily windows around Hanukkah) and loaded one slice a day.
- **Everything as code:** AWS resources in Terraform or CDK, Databricks pipelines and jobs in a bundle, all in GitHub with CI. Free Edition accounts can be deleted after long inactivity, so the repo must rebuild everything.

### Bronze table: `workspace.bronze.price_items`

Managed Delta table (Databricks storage in Ohio), created by `databricks/notebooks/bronze_price_items.py`. One row per product in a PriceFull file, for all 10 chains. Loaded by Auto Loader in batches of 100 files, split into 20 partitions so that only about 5 files' parsed products sit in Python's memory at once. Job `bronze_load` runs this, the stores load and the promotions load. The same file can sit under several `date=` folders, so per-file checks group by `file_path`, and silver removes the repeats.

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

Managed Delta table (Databricks storage in Ohio), created by `databricks/notebooks/bronze_stores.py`. One row per store in a Stores or StoresFull file. It has the same columns as `price_items` without `declared_count`, with these differences in meaning:

| Column | Type | Meaning |
|---|---|---|
| `tags_in_file` | INT | Number of `<Store>` tags in the decompressed file text |
| `header` | MAP<STRING, STRING> | Everything above the `Store` tag: the file's top-level fields and the sub-chain (`SubChainID` or `SubChainId`, by chain) |
| `fields` | MAP<STRING, STRING> | This store's fields, as published |

### Bronze table: `workspace.bronze.promotion_items`

Managed Delta table (Databricks storage in Ohio), created by `databricks/notebooks/bronze_promotion_items.py` (job `bronze_load`, task `load_promotions`). One row per product in a promotion in a PromoFull file, for all 10 chains. A promotion with no products, or a file with no promotions, stays as one row with NULL `item`. A **store-wide** promotion (more than 10,000 products in one file, see below) is also one row with NULL `item`. Loaded by Auto Loader in the same batches as `price_items`.

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
| `items_in_promotion` | INT | Number of products this promotion lists in this file (counted from the tags, filled on every row; 0 for a promotion with none) |
| `store_wide` | BOOLEAN | True when `items_in_promotion` is above 10,000: the promotion is then ONE row with NULL `item` |

**Store-wide promotions (rule decided by Itai 2026-11-12):** a promotion that lists **more than 10,000 products in one source file** is store-wide: a basket-level offer (a donation, a delivery fee, a 30 NIS coupon, "10% discount") that carries the chain's whole catalog. It is kept as one row per source file (store) + promotion, not one row per product; the full list stays in raw S3. Why 10,000: measured on 2026-11-12, real product deals list at most about 9,100 products in the scraper data (and about 5,200 in the Kaggle sample), while store-wide offers start at about 11,500 (Shufersal Cibus coupon 83,475, Keshet "10% discount" 54,669, Carrefour December 2025 offers about 34,000 each, in about 170 store files, which was 17 of 20.9 million rows a day and made the Kaggle promotion load fail after 63 minutes). The count is taken before any product is built (in the XML parser for the scraper, in the SQL for Kaggle), so the big lists are never turned into rows. The rule is the constant `STORE_WIDE_MIN_ITEMS` in `bronze_promotion_items.py` and `bronze_kaggle.py`. One older Kaggle feed (Victory, Mahsani Ashuk) has one promotion + product per row, so a store-wide offer there can't be recognised and stays per product.

One-off clean-up 2026-11-12 of rows loaded before the rule: 405 scraper promotions (Shufersal 21, Tiv Taam 376, Keshet 8) with 5,109,238 product rows were replaced by 405 rows; all Kaggle promotion rows were deleted and their versions set back to `pending` in the queue.

Checked 2026-11-10: all 3,156 promotion files in S3 are loaded, 35,694,531 rows and 5,000,651 promotions, from all 10 chains. No file failed to parse, and every file's product rows and promotions match its tag counts. Two files contain no promotions, so each is one row with NULL `promotion`. The load took about 12 minutes.

### Kaggle rows in bronze

Notebook `databricks/notebooks/bronze_kaggle.py`, job `kaggle_backfill` (tasks `load_kaggle_prices`, `load_kaggle_stores`, `load_kaggle_promotions`, then the whole `silver_load` job, then `log_run`). The three tables above get rows with `source = 'kaggle'` in the same shape as the scraper's, so silver reads both the same way:

| Column | For a Kaggle row |
|---|---|
| `file_path` | S3 path of the Kaggle CSV (`source=kaggle/date=<version date>/chain=<CHAIN>/<name>.csv.gz`). One CSV holds many source files |
| `file_name` | The source file's name, taken from the block's first row (`PriceFull7290785400000-010-202611090010.xml`); silver takes the file's date from it. Per-file checks group by `file_path` + `file_name` |
| `collected_date` | The `date=` folder: the version date |
| `declared_count`, `tags_in_file`, `promotions_in_file`, `items_in_file`, `parse_error` | NULL. There is no XML to count; the row count of each block was checked against an independent count (see below) |
| `header`, `fields`, `promotion`, `item` | Maps with the scraper's field names (`StoreID`, `ItemCode`, `PromotionId`, ...); entries that are empty in the source are left out |
| `promotion_seq` | Position of the row in its block |

How a Kaggle CSV becomes rows: all cells are read as text with `header=false`, because versions have different headers; each file's header is its first row. Every blank cell is filled from the row above inside the same block, in file order (a `.gz` file can't be split, so one task reads it in order); the literal `''` becomes NULL and is never filled. Line ends are mixed in these files (CRLF on the header and some rows, LF on the rest): the reader is told `lineSep = LF`, and the stray CR is trimmed from the last cell. Without that, Spark takes the first line's CRLF as the only separator and merges rows (it lost 11,508 of 325,151 rows of one Shufersal promotion file). Promotion cells hold JSON (single-quoted, Python style, in 2025 versions; double-quoted in 2026; `NO_BODY` = empty element, a single item is an object instead of a list), parsed as `VARIANT`. Three promotion layouts are mapped to the scraper's two: items in `groups` (key spelling of the Groups layout, `PromotionID`), items in `promotionitems` (flat layout, `PromotionId`), and, in one older feed (Victory, Mahsani Ashuk), one promotion + product per row with `itemcode` columns (flat). Two Kaggle spellings of one field are coalesced into the scraper's key (`unitmeasure` and `unitofmeasure` into `UnitOfMeasure`).

**A header that repeats a name (decided by Itai 2026-11-15).** Some Kaggle headers spell one tag twice with different capitalisation, because different stores' XML spell it differently (Rami Levy promotions: `MaxQty` and `MAXQTY`, 2024-12-07 to 2025-01-06; Hazi Hinam prices: `BisWeighted` and `bIsWeighted`, 2024-12-25 to 2025-01-02; a scan of the 868 CSVs of the 30 failed versions found only these two). Lowercased they collide, and Spark stops with `DUPLICATED_MAP_KEY`. Bronze keeps both values: the 2nd occurrence of a name, in header order, becomes `<name>_2` (3rd `_3`, up to `_9`), and it is carried into the same map under the scraper's key plus the suffix (`bIsWeighted_2`, `MaxQty_2`). Nothing is dropped or merged in bronze, and every rename is printed in the task log (`RENAMED repeated header name in <file>: ...`). In the source files measured, only one of the two columns is ever filled inside a block, so there is no real conflict. **Silver resolves it** on the whole map before any field is read (SQL functions `workspace.silver.resolve_dup_keys` and `has_dup_conflict`, created by task `create_functions` of `silver_load`): only `X` filled gives `X`, only `X_2` gives `X_2`, both equal give that value, both filled and different send the row to quarantine with `reject_reason = 'duplicate_column_conflict'` (`silver.quarantine` or `silver.promotion_quarantine`, original maps kept). DQ check `duplicate_column_conflicts`: WARNING above 0, never FAIL.

**JSON key spelling.** The JSON cells of the promotion CSVs (`promotionitems`, `groups`, `clubs`, `additionalrestrictions`) spell their keys in PascalCase in the December 2024 and January 2025 files (`{'Item': {'ItemCode': ..., 'IsGiftItem': ...}}`, `ClubId`, `Clubid`, `AdditionalIsCoupon`) and in lowercase in the November and December 2025 files (`item`, `itemcode`, `clubid`). Variant paths are case-sensitive, so the first version of the loader found no items in the 2024 files and sent every row to quarantine as `no_item_code` (found 2026-11-15). Now every key is looked up by `lower(key)` (`vget` in `bronze_kaggle.py`) and item keys are lowercased before the mapping to the scraper's names; the values are never changed. Promotion files exist only for 2024-12-04 to 2025-01-06 and 2025-11-23 to 2025-12-29. Rows that still end as `no_item_code` are promotions with no product in the source (credit-card or club offers).

**Safety net.** If a chunk of 30 files fails to write, the loader retries that chunk file by file; a file that fails alone becomes one bronze row with `parse_error` set to the error text and every other column NULL (the same shape as the scraper's), and the load goes on. Silver already sends such rows to quarantine as `file_not_readable`.

Loading is driven by the queue (next section), not by listing the bucket: each task deletes the bronze rows of the batch's `date=` folders first, then loads exactly those folders' files, 30 files per write. A re-run of a batch therefore ends with the same rows, never double.

### Backfill queue: `workspace.ops.backfill_queue`

Managed Delta table, one row per Kaggle version to load (162 rows: 157 + 5 extra versions that replace 5 weekly ones (446, 271, 93, 89, 83, now `skipped`) because they hold more chains, seeded from `ingestion/fetch_kaggle/backfill_versions.json` by `databricks/seed_backfill_queue.py`; created by notebook `backfill_queue.py`). Load order: Hanukkah 2025 window, Hanukkah 2024 window, then weekly, newest to oldest.

| Column | Type | Meaning |
|---|---|---|
| `version` | INT | Kaggle dataset version number |
| `version_date` | DATE | Upload date = the `date=` folder in the Ohio bucket |
| `load_order` | INT | 1 is loaded first |
| `kinds` | STRING | File kinds to load, comma separated (`price_full_file,store_file[,promo_full_file]`) |
| `status` | STRING | `pending`, `running`, `done`, `failed` or `skipped` (never claimed: a better version replaces it) |
| `attempts` | INT | How many times a batch with this version was started. At 3, a `failed` batch is skipped |
| `batch_id`, `job_run_id` | STRING | The `kaggle_backfill` run that claimed it |
| `started_at`, `ended_at` | TIMESTAMP | Claim and finish time |
| `rows_price`, `rows_promo`, `rows_stores` | BIGINT | Rows that version has in the three bronze tables after the load |
| `error` | STRING | The failed task and its message |

**One run of job `kaggle_backfill` is one batch** (job parameter `batch_size`, default 5 versions). Tasks, in a line: `claim_batch` (marks the batch `running`; says `go = no` when there is nothing to do or `daily_load` is running) → `has_batch` (condition; on `no` the rest is skipped and the run still succeeds) → `load_kaggle_prices` → `load_kaggle_stores` → `load_kaggle_promotions` → `run_silver` (the whole `silver_load` job) → `finish_batch` (`done` with row counts, or `failed` with the error) → `log_run`. The tasks are sequential, so the job never needs more than about 3 concurrent tasks of the 5 the Free Edition allows.

- **First batch, tested by hand 2026-11-12** (versions 418, 417, 416, 414, 413: the Hanukkah 2025 window, price + store + promo): 24 minutes in all. `load_kaggle_prices` 189 s, `load_kaggle_stores` 21 s, `load_kaggle_promotions` 283 s, `run_silver` 842 s, the rest under a minute. Bronze rows added: about 35.3 million price rows (about 7.1 million a version) and 211 million promotion rows (about 42 million a version, December 2025 promo files are large). Promotion-heavy batches are the slow ones; weekly versions have no promotion files and should take a fraction of that. DQ after the batch: 18 PASS, 2 WARNING, no FAIL.
- **Schedule:** every 30 minutes, Asia/Jerusalem, `max_concurrent_runs` 1. If the daily quota stops compute, the batch stays `running` or `failed`, and the first run after the quota resets claims the same batch again (the bronze rows of a half-loaded batch are deleted first). A batch that fails 3 times is left `failed` and the queue moves on; retry it with `UPDATE workspace.ops.backfill_queue SET attempts = 0 WHERE status = 'failed'`.
- **Overlap and hangs:** `queue.enabled: false`, so a schedule tick that finds the previous batch still running is dropped, not queued. The three load tasks have a 40-minute timeout (normal 20 s to 5 min). On 2026-11-12 the promotion load of batch 2 (versions 412-407) failed after 63 minutes with an Databricks INTERNAL_ERROR and was retried by Databricks; cause unknown (not a code error).
- **Where we stand:** `SELECT status, count(*), sum(rows_price) FROM workspace.ops.backfill_queue GROUP BY status`.
- **Pause:** in `databricks/databricks.yml` set `schedule.pause_status: PAUSED` for `kaggle_backfill` and `bundle deploy` (from `main`), or pause the schedule in the Jobs UI. **Resume:** set `UNPAUSED` and deploy. The queue keeps its state, so nothing is lost either way. When the queue is empty the schedule can be paused for good; every run then costs only the small `claim_batch` task.
- **Add or redo a version:** insert a row (`status = 'pending'`) or set `status = 'pending'` on an existing one. A reload is safe: the batch deletes the version's bronze rows first, and the silver MERGEs are idempotent.
- **Run one batch by hand:** `databricks bundle run kaggle_backfill` (add `--params batch_size=2` for a smaller one). Not while `daily_load` runs.

Checked 2026-11-12 on slice 00 (two versions: 405, dated 2025-12-15, price + store + promo; 720, dated 2026-11-12, price + store): every source file in the 5,102 blocks (3,532 price, 1,544 promotion, 26 store) has exactly as many bronze rows as rows in its block, counted with Python's `csv` module (23,870,219 price rows, 2,285,907 promotion rows, 2,371 store rows). The blank fill was compared with an independent pandas fill on 931 price files of 5 chains and both versions: row counts, price sums and a CRC of 26 columns per row are equal in all 931.

Silver over slice 00 (job run 703778780511446, 2026-11-12): 5,902,790 Kaggle rows reached `price_obs` (the rest are repeats inside a file or days our scraper already has), 1,327,292 reached `promotion_obs`, 10,552 price rows went to `quarantine`, and 464,142 promotion rows to `promotion_quarantine`, of which 464,137 were `wrong_chain`: the promotion files of Victory and Mahsani Ashuk's older feed carry chain id `72906`, not the 13-digit id. Fixed 2026-11-12 (decided by Itai): the original file name inside each block still carries the full id, so silver takes it from there (see "Chain id from the file name"); a re-read moved 343,950 rows into `promotion_obs` (the rest are older files for the same store and day, or repeats inside a file), and the 464,137 rows were removed from `promotion_quarantine`. `price_history` was rebuilt in full in 38 s (12.0 million periods). DQ: no FAIL, the same two WARNINGs as before. `kaggle_vs_scraper_file_mismatch` is 0: 1,315 source files exist in both sources (v720 against our scraper, 10.85 million rows each), and every one has the same row count and price sum in both.

Wall time per task: `load_kaggle_prices` 208 s, `load_kaggle_promotions` 86 s, `load_kaggle_stores` 69 s (run in parallel, 208 s together), `silver_load` 292 s (price_obs 105, promotion_obs 85, DQ 69, items 64, price_history 38).

### Silver table: `workspace.silver.stores`

Managed Delta table, rebuilt in full on each run by `databricks/notebooks/silver_stores.py` (job `silver_load`, not scheduled yet). One row per `chain` + `store_id`: the store's newest version in `bronze.stores` among our own collection (`source = 'scraper'`), by the date in the file name and then `modified_at`. Older versions of a store aren't kept here; they stay in bronze. Kaggle store files (history) are loaded into `bronze.stores` only, so this table stays the current store list.

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
| `city_code_resolved` | INT | The store's Central Bureau of Statistics (CBS) locality code, found by the city rule below; NULL when none is found |
| `city_name_resolved` | STRING | CBS Hebrew name of `city_code_resolved` |
| `city_name_en` | STRING | CBS English name |
| `district_name` | STRING | CBS district (Hebrew) |
| `city_source` | STRING | Which rule found the city: `code`, `code_alias`, `name`, `store_name`, `address`, or `none` |
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
- **`city_code` is the CBS locality code** for the chains that publish a number; `city_code_resolved` checks it against the CBS list (seed below) instead of trusting it.

**City rule (added November 18th, 2026).** `silver_stores.py` resolves each store's city in this order; the first hit wins and is recorded in `city_source`:

1. `code`: the published numeric city code (not 0) is in the CBS locality list.
2. `code_alias`: the code is a known wrong code listed in the aliases (`match_type = code`).
3. `name`: `city_name` text, after normalizing (hyphens, quotes, dots, commas and brackets become spaces; `יי` becomes `י`, `וו` becomes `ו`; spaces collapsed, then removed for the comparison), equals a CBS name or a name alias (`match_type = name`).
4. `store_name`: a locality name of 3 or more letters (the longest first) appears as whole words in `store_name`. Words `אזור`, `רחוב`, `מרכז`, `כרמים` are skipped.
5. `address`: the same in `address`, also skipping `שדרות` and `מבקיעים`.
6. `none`.

Seeds in `databricks/seed/`: `cbs_localities.csv` (the CBS locality list, 1,316 rows: code, Hebrew and English name, subdistrict, district, regional council) and `city_aliases.csv` (`match_type`, `value`, `city_code`, `note`; extra spellings and wrong codes, edited by hand). The notebook stops if an alias points at a code that is not in the CBS list. Tested on the 849 in-scope stores on November 18th, 2026: 835 get a city and 14 stay `none` (Yohananof 9, Rami Levy 2, Victory 2, Shufersal 1; mostly Yohananof stores with `unknown` addresses). `is_eilat` is unchanged; the notebook prints it next to `city_code_resolved = 2600` for comparison.

### Silver table: `workspace.silver.price_obs`

Managed Delta table, clustered by `file_date` and `chain`, built by `databricks/notebooks/silver_price_obs.py` (job `silver_load`, task `build_price_obs`, which runs after `build_stores`; not scheduled yet). One row per `chain` + `store_id` + `item_code` + `file_date`: what the chain published for that product on that day. It is an observation, not proof of the shelf price. Product names are not here; they go in `items`, so they aren't repeated about 7.4 million times a day.

**Loading:** each run reads only bronze rows newer than the table's latest `bronze_loaded_at`, and saves them with `MERGE` on the key above. An existing row is replaced only by a later file from the same source, or by a scraper row replacing a Kaggle row. When one load has several candidates for a key, the scraper wins, then the latest file. To rebuild, drop the table and run the job. If `quarantine` is empty, the run reads all of bronze to fill it, and rows already in `price_obs` don't change.

**Rejected rows go to `silver.quarantine`:** files that failed to parse; a chain ID that differs from the chain's ID in `silver.stores` and isn't a documented alias for that date (see "Chain id aliases"); no file date, store ID or barcode; a price that isn't a positive number. The same notebook decides both outcomes, and saves rejects first, because saving prices moves the watermark. Prices from stores missing from the Stores file are kept.

| Column | Type | Meaning |
|---|---|---|
| `chain` | STRING | Chain, from the S3 folder |
| `source` | STRING | `scraper` or `kaggle` |
| `chain_id` | STRING | Chain ID published in the file |
| `sub_chain_id` | STRING | Sub-chain ID published in the file |
| `store_id` | STRING | Store ID without leading zeros, taken from the file name (see "Store id from the file name"); joins to `silver.stores` with `chain` |
| `store_id_header` | STRING | The StoreID inside the file, without leading zeros, kept for checking; can differ from `store_id` (Carrefour). NULL in rows loaded before November 17th, 2026 and not reloaded |
| `store_id_source` | STRING | `file_name`, or `header` when the file name had no store part (a FAIL in the DQ check `store_id_from_header_fallback`) |
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
| `reject_reason` | STRING | `file_not_readable`, `duplicate_column_conflict` (Kaggle: a repeated header name filled with different values, see "A header that repeats a name"), `wrong_chain`, `no_file_date`, `no_store_id`, `no_item_code`, `price_not_a_number` or `price_zero_or_less` |
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

### Silver table: `workspace.silver.store_wide_promotions`

Managed Delta table, clustered by `file_date` and `chain`, filled by the same notebook and task as `promotion_obs` (`silver_promotion_obs.py`, task `build_promotion_obs`) from the bronze rows with `store_wide` = true: promotions that list more than 10,000 products in one source file (rule and the measured gap in the bronze section). One row per `chain` + `store_id` + `promotion_id` + `file_date`, saved with `MERGE` on that key; the scraper beats Kaggle, and inside one source the later file wins, as in `promotion_obs`. Its own watermark (latest `bronze_loaded_at` in this table). The products are not listed; `items_in_promotion` says how many there were.

Not used by gold (decided by Itai 2026-11-17: coupons, donations and delivery fees are not deals on products). `discounted_price` here is one listed product's price, a different product each day, so it means nothing at this grain; `items_in_promotion` grows as the chain adds products. Grouping these rows by every column would split one promotion into a row per day.

| Column | Type | Meaning |
|---|---|---|
| `chain`, `chain_id`, `store_id` | STRING | As in `promotion_obs` (store id without leading zeros; chain id from the file name when the header's is cut off) |
| `promotion_id` | STRING | Without leading zeros |
| `description` | STRING | Promotion text, for example the coupon's name |
| `start_at`, `end_at` | TIMESTAMP | Promotion validity |
| `club_code` | STRING | First number in the club field |
| `reward_type` | STRING | Promotion-level reward type, when given |
| `discount_rate` | DECIMAL(12,4) | Promotion-level discount, when given (not in the Groups layout, where it sits on the products) |
| `discounted_price` | DECIMAL(12,2) | Promotion-level price, when given |
| `items_in_promotion` | INT | Number of products the promotion listed in that file |
| `file_date`, `file_time` | DATE, STRING | From the original file name |
| `source` | STRING | `scraper` or `kaggle` |
| `source_file` | STRING | Path of the file that carried it |
| `bronze_loaded_at` | TIMESTAMP | When bronze loaded it |

Rows that fail the same checks as `promotion_obs` (no file date, store ID or promotion ID, wrong chain) go to `silver.promotion_quarantine` with a NULL `item`. DQ check `store_wide_promotions_latest`: the most distinct store-wide promotions any chain had on the latest promotion day, WARNING above 20, never FAIL.

### Silver table: `workspace.silver.promotion_obs`

Managed Delta table, clustered by `file_date` and `chain`, built by `databricks/notebooks/silver_promotion_obs.py` (job `silver_load`, task `build_promotion_obs`, which runs after `build_stores`; not scheduled yet). One row per `chain` + `store_id` + `promotion_id` + `group_id` + `item_code` + `file_date`: what the chain published about that product in that promotion on that day. It is an observation, and it says nothing about whether the deal is really cheaper; gold answers that by comparing with `price_obs`.

**Loading:** the same as `price_obs`: only bronze rows newer than the table's latest `bronze_loaded_at`, saved with `MERGE` on the key above. An existing row is replaced only by a later file from the same source, or by a scraper row replacing a Kaggle row. To rebuild, drop the table and run the job.

**Chain id aliases:** table `silver.chain_id_aliases` (`chain`, `chain_id`, `valid_from`, `valid_to`, `reason`), created by `silver_functions`, lists a second chain ID a chain may publish under, and for which file dates. The `wrong_chain` check in `price_obs`, `promotion_obs` and `items` accepts it inside that window; rows keep the ID they were published with. One row today: `YAYNO_BITAN_AND_CARREFOUR` accepts `7290725900003` (Yayno Bitan's own ID) up to 2025-07-13. Found 2026-11-16 in the final backfill checks: Kaggle's files for this chain carry the old ID from 2024-10-21 to 2025-07-07 (28 stores, shrinking to 3 as branches moved to the Carrefour feed) and the Carrefour ID `7290055700007` from 2025-07-14; Kaggle has no Carrefour-ID files before that, so those months cover only the stores still on the old feed. Checked before accepting (decided by Itai): all 19 store numbers seen under both IDs have the same address and ZIP in the Stores files (only the names changed, e.g. store 30 "מהדרין אילת" → "קרפור מרקט אילת"); the other 9 numbers are never used by the new ID, so no two stores merge. Reloaded with `silver_load` tasks `create_functions,build_price_obs,build_promotion_obs,build_items` and notebook parameter `reload_chain=YAYNO_BITAN_AND_CARREFOUR` (re-reads bronze for one chain only; `reload_after` optional): 2,842,877 rows into `price_obs`, 688,317 into `promotion_obs` (21 stores, 2024-12-06..2025-01-05), then the matching `wrong_chain` rows were deleted from both quarantine tables (30,802 promotion rows without a barcode were relabelled `no_item_code`). `price_history` grew by 510,052 periods; Carrefour's Eilat stores 30 and 1252 now start 2024-10-21.

**Chain id from the file name:** when the header's `ChainID` is shorter than 13 digits, silver takes the 13-digit id from the original file name (`PromoFull<chain id>-<store>-...`) instead. The `wrong_chain` check still compares it with the folder's chain, so a file in the wrong folder is still caught. Needed for Kaggle's 2025 Victory and Mahsani Ashuk promotion files, whose header says `72906`. A one-off re-read after a rule change: run `build_promotion_obs` with notebook parameter `reload_after=<timestamp>`.

**Store id from the file name (price_obs, promotion_obs, store_wide_promotions, items):** `store_id` comes from the file name, not from the `StoreID` inside the file. Decided November 17th, 2026 (found by Itai and Claude from raw files): Carrefour (`YAYNO_BITAN_AND_CARREFOUR`) puts an OLD store code in the header of many files. On November 17th, 2026, 78 of 152 price files differed; for example `PriceFull7290055700007-001-400-20261117-051002.xml.gz` is store 400 ("קרפור סיטי קניון כפר סבא") but its header says 103. 19 of those old codes are the ID of another real store (old code 420 is Carrefour Market Eilat), so two stores' prices landed under one key and the day's dedup kept only one. The file name matches the chain's Stores file. Of the other nine chains, a sample of three price and three promotion files each agreed with their header, except Yohananof's 19 stale files named `PriceFull...-7999-2024...xml.gz` (header 799; a test store, files of 2024 still listed on the site). The SQL function `workspace.silver.store_id_from_file_name` (in `silver_functions.py`, with a self-test) takes the last number before the date: `<Price|Promo>Full<13-digit chain id>-[<sub-chain>-]<store>-<yyyymmdd>-<hhmmss> or <yyyymmddhhmm>`, leading zeros dropped, case ignored; it covers the three formats seen (with sub-chain, Rami Levy and Keshet's `-<store>-<12 digits>`, Yohananof's `-<store>-<12 digits>`), and Kaggle rows too (their `file_name` is the original file's name). The header value is kept in `store_id_header`; `store_id_source` is `header` only when the name has no store part. DQ checks `store_id_header_mismatch` (WARNING above 0, expected for Carrefour) and `store_id_from_header_fallback` (FAIL above 0). `silver.stores` is unchanged: a Stores file lists one `Store` element per store. Rebuild after the change: notebook parameters `reload_chain=YAYNO_BITAN_AND_CARREFOUR` and `rebuild_chain=yes` first delete the chain's rows in `price_obs`, `quarantine`, `promotion_obs`, `store_wide_promotions` and `promotion_quarantine`, then reload them from bronze (a plain reload cannot fix them, because the key changed); `price_history` rebuilds in full from `price_obs` on its next run.

**Matching the two layouts:** `promotion_id` loses its leading zeros, because Keshet publishes the same promotion as `1145634` in one layout and `0001145634` in the other. The flat layout has no groups, so its `group_id` is `1`. Reward type, quantities, price and discount come from the product in the Groups layout and from the promotion in the flat layout.

**Repeats inside one file:** a product can appear twice in the same group of a promotion, usually once at 0.00 and once at its real price (Shufersal, 183,548 of its 183,652 repeats). The row with a positive price is kept. If both are positive, the lower price is kept (98 cases). The official meaning of these pairs isn't confirmed.

**Store-wide promotions are not here:** bronze rows with `store_wide` = true have no product, so this task skips them (they are not rejected as `no_item_code`) and writes them to `silver.store_wide_promotions` instead.

**Rejected, stored in `silver.promotion_quarantine`:** a file that failed to parse, a Kaggle row whose repeated header name holds different values (`duplicate_column_conflict`), a file with no promotions, a chain ID that differs from the chain's ID in `silver.stores`, no file date, store ID, promotion ID or barcode, or a negative price. It has the columns of `silver.quarantine` plus `promotion_id`, with `promotion` and `item` in place of `fields`, and it is separate because the price checks in `silver_dq` count the rows of `silver.quarantine`. A price of 0 or a missing price isn't rejected: many promotions give only a discount percent or a gift.

| Column | Type | Meaning |
|---|---|---|
| `chain` | STRING | Chain, from the S3 folder |
| `source` | STRING | `scraper` or `kaggle` |
| `chain_id` | STRING | Chain ID published in the file |
| `sub_chain_id` | STRING | Sub-chain ID published in the file |
| `store_id` | STRING | Store ID without leading zeros, from the file name as in `price_obs`; joins to `silver.stores` with `chain` |
| `store_id_header`, `store_id_source` | STRING | As in `price_obs` (also in `store_wide_promotions`) |
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

Checked 2026-11-10: of 35,694,531 bronze rows, 31,261,575 kept, 4,432,950 repeats dropped, 6 rejected (2 files with no promotions, 4 negative prices), which adds up to the bronze total. No repeated key. 10 chains, 981 chain + store pairs, 44 file dates, of which 3 are full days; the others come from old files still listed on the sites. A full day is about 11.5 million rows, and the table is 481 MB. The first load took under 3 minutes; a second run read 0 rows and inserted 0. 14 stores publish promotions but aren't in `silver.stores` (Shufersal 13, Keshet 1). 104 rows had already expired on their file date. `silver_dq` now has promotion checks (see the DQ section).

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
| `chain_without_price_files` | file | - | 0 | 0, PASS |
| `kaggle_vs_scraper_file_mismatch` | file | 0 | - | see below |
| `price_store_completeness` | file | 0 | - | 0, PASS |
| `store_id_header_mismatch` | row | 0 | - | new November 17th, 2026; latest day's scraper rows whose header StoreID differs from the file name's, WARNING expected while Carrefour publishes old codes |
| `store_id_from_header_fallback` | row | - | 0 | new November 17th, 2026; latest day's rows whose `store_id` had to come from the header |
| `duplicate_column_conflicts` | row | 0 | - | new 2026-11-15; rows with `duplicate_column_conflict` in `quarantine` and `promotion_quarantine`, WARNING above 0 |
| `promotion_repeated_keys` | row | - | 0 | 0, PASS |
| `promotion_reject_percent` | row | 0.1 | 5 | 0.00003, PASS |
| `promotion_expired_percent` | business | 0.1 | - | 0, PASS |

`price_obs_without_item` looks at the latest day only. The limit of 0 on items without a name is measured (0 today), so any missing name shows as a WARNING. The limit of 100 on price jumps comes from one measured day and is provisional. The stop-on-FAIL path has not been seen in a real run yet. The row-count-per-file check (±30% of the 7-day average) is not built yet.

**Price completeness (S6).** A run can report `ok` and still miss files, because the scraper swallows listing timeouts (a Shufersal run once missed 24 of 417 price files). Two checks compare each chain's newest Stores file (on or before the latest price date, physical stores only, as in `silver.stores`) with the stores that have rows in `price_obs` on a date. Measured 2026-11-08 to 2026-11-10 (stores listed, stores missing a price file): Shufersal 416, 1; Rami Levy 98, 0; Carrefour/Yeinot Bitan 144, 60 (about 42% every day, so it is that chain's normal level); Mahsani Ashuk 64, 0 to 1; Victory 67, 0 to 1; Yohananof 50, 3; Keshet 23, 1; Osher Ad 24, 0 to 1; Tiv Taam 47, 0; Hazi Hinam 12, 0. Because each chain has its own normal level, `price_store_completeness` compares a chain with itself: it counts the chains whose missing stores on the latest date exceed the median of the earlier days in the last 7 (days the chain published) by more than 2 stores and more than 3% of its stores, and gives a WARNING above 0. A chain with no earlier day is compared with itself, so it cannot warn on its first day. The Shufersal incident (24 extra missing, 5.8%) would trigger it; a single store of Osher Ad (4%) does not. `chain_without_price_files` is a FAIL above 0: a chain that lists stores but has no price rows at all on the latest date. Limits: "published" means rows in `price_obs`, so a store whose file was rejected completely counts as missing; the latest date is the newest `file_date` in `price_obs`, so a stray file with a later date would make it count every chain as missing.

**Promotion checks.** They use the newest `file_date` in `promotion_obs`. `promotion_repeated_keys` (key `chain` + `store_id` + `promotion_id` + `group_id` + `item_code`, FAIL above 0); `promotion_reject_percent` is `promotion_quarantine` rows over kept plus rejected rows of that day (WARNING above 0.1, FAIL above 5, the same limits as for prices; 3 of 11.3 million rows on 2026-11-10); `promotion_expired_percent` is rows whose `end_at` is before the file date (0 on 2026-11-10, 104 rows or 0.001% on 2026-11-09), WARNING above 0.1 and never FAIL, since a chain may keep an ended promotion in the file. `store_wide_promotions_latest` is the most distinct store-wide promotions any chain had on that day (WARNING above 20, never FAIL).

### Silver tables: `workspace.silver.product_categories` and `product_categories_pilot`

`silver.product_categories` is the final category table: one row per `item_key` (`category`, `category_group`, `is_food`, `confidence`, `source`). It is created empty by `gold_dims.py` and filled in step G1e (rules plus `ai_classify` on active products), so `gold.dim_product` already joins it and its category columns are NULL until then.

`silver.product_categories_pilot` is a pilot, overwritten on every run of the job `classify_products` (notebook `silver_product_categories.py`, parameters `mode` = `pilot`, `sample_size` = 1000; not part of `daily_load`). It holds 1,000 active products (last seen within 14 days of the newest `last_seen`): 500 in 3 or more chains (`multi_chain`) and 500 in fewer (`other`), picked by a hash of `item_key` so a rerun picks the same ones. Input text: name, then the manufacturer unless empty or a placeholder, then quantity and unit. Columns: `item_key`, `stratum`, `input_text`, `category`, `confidence`, `category_group`, `is_food`, `error_message`, `raw_result`, `model_version`, `classified_at`. `ai_classify` (version 2.1, with confidence scores) returns a VARIANT, so the result is written to a table, never a view. The 24 labels (Hebrew descriptions are in the notebook), by group: `food`: `bread_bakery`, `grains_pasta_legumes`, `meat_poultry`, `fish`, `dairy_eggs`, `oils_fats`, `sweets_snacks`, `coffee_tea`, `soft_drinks`, `alcohol`, `prepared_other_food`, `baby_food`; `fresh_produce`: `fresh_fruit`, `fresh_vegetables`; `nonfood`: `baby_care`, `cleaning_laundry`, `paper_disposables`, `personal_care`, `health_pharma`, `pet`, `home_kitchen`, `toys_stationery_seasonal`, `tobacco`, `other_nonfood`. `is_food` is true for `food` and `fresh_produce`.

Round 2a (evaluation): `databricks/seed/category_eval.csv` is a hand-labelled test set of 240 products (`eval_set` = `round1_review`, 120 products checked by hand after round 1, or `new_blind`, 120 new active products labelled before any model saw them; 6 rows are `AMBIGUOUS` and are left out of every score). The notebook's `mode=eval` joins the CSV to `silver.items` (all 240 must match), builds a cleaner input text (the longest name in `names`; the manufacturer only when it is a real value; quantity and unit only when clean), and runs three methods on the same text: `ai_classify_v2` (version 2.1 with a rule list as `instructions`), and `ai_query` with `databricks-gpt-oss-120b` and with `databricks-qwen35-122b-a10b` (same rules and label descriptions in the prompt, JSON answer with an enum of the 24 keys). The answers go to `silver.product_categories_eval` (`item_key`, `eval_set`, `method`, `input_text`, `category`, `confidence`, `raw_result`, `run_at`; overwritten on every run); the shared inputs are kept in `silver.product_categories_eval_input`. Run with `databricks bundle run classify_products --params mode=eval` (job parameter; the notebook widget `mode`).

Full run (`mode=full`, prompt version `v2`, model `databricks-gpt-oss-120b`): classifies every active product (last seen within 14 days of the newest `last_seen`) except products sold only in Shufersal Be stores (`silver.stores.sub_chain_id = '5'`; Be is out of gold), most-shared products first (`chains_count` descending). It uses exactly the eval prompt and input text v2, and is resumable: results go to `silver.product_categories_ai` (`item_key`, `model`, `prompt_version`, `input_text`, `category`, `raw_result`, `error_message`, `batch_id`, `classified_at`; successes only, never replaced), so a rerun only picks products with no row yet for that model and prompt version. Each batch of `batch_size` (default 5000) is logged in `silver.product_categories_ai_batches`; no new batch starts after `max_minutes` (default 150); a batch with over 20% errors stops the run as FAILED with the sample error. The scope is kept in `silver.product_categories_ai_scope` (rebuilt every run). Start: `databricks bundle run classify_products --no-wait --params mode=full`. Progress: `SELECT model, prompt_version, count(*) FROM workspace.silver.product_categories_ai GROUP BY 1,2` and `SELECT * FROM workspace.silver.product_categories_ai_batches ORDER BY started_at`.

Run on November 17th, 2026: 1,000 rows, 0 errors, the classification statement took 223 s (job run 704827695797905; check the cost later in `system.billing.usage`, which lags hours). Confidence has 34 distinct values (0.35 to 0.96, median 0.78); 5 rows have a NULL score, all `toys_stationery_seasonal`. Accuracy has not been judged yet.

## Gold: the star (milestone 4)

Design, lineage and the questions it answers: the "Gold layer design" section of `docs/architecture.html` (claude.ai/artifact/6fuqxaVS7aWoaqWakoTAUV). Built by `databricks/notebooks/gold_facts.py` (facts) and `gold_dims.py` (dimensions), job `gold_load` (tasks `build_facts`, `build_dims`, `log_run`; the two builds are independent), which `daily_load` starts after `silver_load` succeeds, so a DQ FAIL in silver stops gold. Rule: a fact is a stored table only when building it changes the grain; otherwise it is a view over silver. Marts read only the star.

**Product key rule:** SQL functions `workspace.silver.item_scope(item_code, item_type)` and `workspace.silver.item_key(chain, item_code, item_type)`, created by `silver_functions.py` (with a self-test). `silver_items` and gold both call them, so the S5 rule lives once: the code itself when shared (ItemType 1, or 8/12/13 digits), else `CHAIN:code`. `store_key` = `CHAIN:store_id`.

### Gold view: `workspace.gold.fact_price_period`

View over `silver.price_history`, one row per store × product × price period. Columns: `store_key`, `product_key`, `price`, `unit_price`, `quantity`, `unit_of_measure`, `is_weighted`, `valid_from` (= `first_seen`), `valid_to` (= `last_seen`), `days_observed`, `is_current`. Checked 2026-11-17: 110,590,696 rows, the same as silver.

### Gold table: `workspace.gold.fact_promotion_period`

Managed Delta table, clustered by `store_key`, `product_key`, rebuilt in full each run (`CREATE OR REPLACE`; incremental "extend or insert" later). One row per `store_key` + `promotion_id` + `group_id` + product, from the daily rows of `silver.promotion_obs`. A promotion ID is the deal's identity, so a row is never split by its values (decided by Itai 2026-11-17): chains re-encode the same deal from day to day (Shufersal's 1+1 toothpaste was 50% / 19.90 on 11-09, no rate on 11-10, 100% / 0.00 from 11-11), and splitting would count one deal several times. A product in several promotions at once has a row for each (on 2026-11-12, 81% of Shufersal's promoted products and 17% of Rami Levy's were in 2 or more).

| Column | Meaning |
|---|---|
| `store_key`, `product_key`, `promotion_id`, `group_id` | The row key (product key from the item type of the last day seen) |
| `description`, `reward_type`, `min_qty`, `max_qty`, `deal_price` (`discounted_price`), `deal_unit_price` (`discounted_price_per_unit`), `discount_rate`, `discount_type`, `club_code`, `is_coupon`, `is_gift_item`, `allow_multiple_discounts`, `min_purchase_amount`, `start_at`, `end_at`, `source` | Values from the last day the deal was seen |
| `first_seen`, `last_seen`, `days_observed` | First and last file date, and the number of days seen (a gap shows as `days_observed` < `last_seen` − `first_seen` + 1) |
| `values_changed` | True when any deal value above differed between days; the day-by-day detail stays in `promotion_obs` |

The notebook stops with an error unless `sum(days_observed)` equals the rows of `promotion_obs` and the price view has the rows of `price_history`. First run 2026-11-17 (2 min 47 s): 23,503,304 deals from 180,136,889 daily rows; 5,577,567 (24%) have `values_changed`. Per column, the share of deals whose value changes is largest for `min_purchase_amount`, `max_qty`, `end_at` (each about 8-9%), `discounted_price` and `discount_rate` (about 4-5%); most are re-encodings, some real (an extended end date). Working out the real price per unit of a deal (1+1 = half price) is the fake-promotions mart's job.

### Gold table: `workspace.gold.dim_date`

Managed Delta table, rebuilt in full by `gold_dims.py` (task `build_dims`) in every `gold_load` run, because `chains_with_prices` changes every day. One row per calendar day, September 1st, 2024 to December 31st, 2027 (1,217 rows, key `date`); the notebook stops if the dates repeat, the count differs or 2026-11-17 is not weekday 3.

| Column | Type | Meaning |
|---|---|---|
| `date` | date | The day |
| `year`, `month`, `day_of_month` | int | Calendar parts |
| `month_name`, `weekday_name` | string | English names |
| `weekday` | int | Israeli week: 1 = Sunday ... 7 = Saturday |
| `is_weekend` | boolean | Friday or Saturday |
| `week_start` | date | The Sunday on or before the date |
| `holiday_name` | string | English holiday name when the date is inside a holiday span (eve to last day, inclusive), else NULL |
| `is_holiday`, `is_hanukkah` | boolean | `holiday_name` is not NULL; `holiday_name` is `hanukkah` |
| `next_holiday_name`, `days_to_next_holiday` | string, int | The first holiday whose start (eve) is strictly after the date, and the days until then. On a holiday day, or on its eve, this is the following holiday |
| `hanukkah_year`, `days_from_hanukkah_start` | int | Nearest Hanukkah (its starting year) and the signed days from its start (eve): negative before, 0 on the eve. For the Hanukkah index window -21 to +7. NULL when the nearest start is more than 60 days away |
| `chains_with_prices` | int | Distinct chains with rows in `silver.price_obs` for that `file_date` (0 when none, also for future dates) |
| `data_coverage` | string | `full` = 8 or more of the 10 chains, `partial` = 1 to 7, `none` = 0 |

**Holiday seed.** `databricks/seed/holidays.csv` (committed) is made on the laptop by `databricks/seed/make_holidays.py`, which computes the dates with the Hebrew-calendar library `convertdate` and cross-checks every date against `hdate` and `holidays` (Israel); it exits with an error on any disagreement. Databricks only reads the CSV: the bundle syncs `seed/` next to the notebooks and `gold_dims.py` reads `../seed/holidays.csv`. Columns: `holiday_name` (`rosh_hashana`, `yom_kippur`, `sukkot`, `hanukkah`, `purim`, `passover`, `independence_day`, `shavuot`), `holiday_name_he`, `holiday_year` (the Gregorian year of `start_date`), `start_date`, `end_date`. **Eve rule:** `start_date` is the EVE, the date whose evening the holiday begins, because shoppers buy before; `end_date` is the last day. Sukkot runs through Simchat Torah (22 Tishri in Israel), Passover is all 7 days, Purim is the 14th of Adar (Adar II in a leap year, Shushan Purim not included), and for Independence Day the start is Memorial Day (the observed day moves: from Friday or Saturday to Thursday, from Monday to Tuesday) and the end is the observed day. Hanukkah spans: December 25th, 2024 to January 2nd, 2025; December 14th to 22nd, 2025; December 4th to 12th, 2026; December 24th, 2027 to January 1st, 2028. The seed covers every holiday that touches the table's date range; to extend the range, change the years in the script, run it and commit the CSV.

Checked 2026-11-17: 1,217 rows and distinct dates; coverage `full` on 171 days (October 21st, 2024 to today), `partial` on 71 (the earliest backfilled days, January 11th, 2025 to November 14th, 2026), `none` on 975 (including all future dates); 116 days fall in the Hanukkah window -21 to +7 (counting all four Hanukkah years).

### Gold table: `workspace.gold.dim_product`

Managed Delta table, clustered by `product_key`, rebuilt in full by `gold_dims.py` (task `build_dims`) in every `gold_load` run. One row per product of `silver.items` (key `product_key`, the same string as `item_key`: the barcode when shared, `CHAIN:code` when chain-scoped). The notebook stops if the row count differs from `silver.items` or `product_key` repeats.

| Column | Type | Meaning |
|---|---|---|
| `product_key` | string | The product key (`silver.items.item_key`) |
| `item_scope`, `item_code` | string | `shared` or `chain`; the code as published |
| `chain` | string | Only for chain-scoped keys; NULL when shared |
| `product_name`, `manufacturer`, `country` | string | From `silver.items` (`item_name`, `manufacturer_name`, `manufacture_country`) |
| `quantity`, `unit_qty`, `unit_of_measure`, `is_weighted` | decimal(12,3), string, string, boolean | Package size, as in silver |
| `chains_count` | int | Chains that published the product |
| `first_seen`, `last_seen` | date | First and last file date |
| `is_active` | boolean | `last_seen` within 14 days of the newest `last_seen` in the table |
| `category`, `category_group`, `is_food`, `category_confidence`, `category_source` | string, string, boolean, double, string | LEFT JOIN to `silver.product_categories`; all NULL until step G1e fills that table (the pilot table is not joined) |

Checked November 17th, 2026: 324,565 rows (296,607 shared), 127,414 active, 0 with a category.

### Silver table: `workspace.silver.store_size_signals`

A helper for `dim_store`, not for Power BI, and it decides nothing yet: no column says a store's final size. Managed Delta table, rebuilt in full by `databricks/notebooks/silver_store_size_signals.py` (job `silver_load`, task `build_store_size_signals`, after `build_stores` and `build_price_obs`; `log_run` waits for it). It was a gold table built inside `gold_dims.py` until November 18th, 2026; it reads only silver tables, so it moved to silver (version 2), and the notebook drops the old `workspace.gold.store_size_signals`. One row per `chain` + `store_id`, for physical stores (`silver.stores.is_physical`) except Shufersal Be (`sub_chain_id = '5'`). The notebook stops if a key repeats or the row count differs from the in-scope stores. It carries `city_name_resolved` and `district_name` from `silver.stores`.

**Signal 1, product count (v2 rules).** `days_observed` = distinct `file_date` of the store in `silver.price_obs` in the last 30 days; `median_products` = median over those days of the price rows per day. `chain_ratio` = `median_products` / the 75th percentile of `median_products` over the chain's stores with 5 or more days (produce stores left out); it is information only and no rule uses it.

- `suspect`: only when the store has fewer than 5 days of files or no price rows at all (reason `fewer than 5 days of files`). A low product count alone no longer makes a store suspect (the old `ratio < 0.25` rule is removed).
- Gap search, per chain, over the stores with 5 or more days (produce stores left out): sort by `median_products` and look only at split points with at least 3 stores on each side. The split with the largest ratio (the upper neighbour divided by the lower one) wins. If that ratio is at least 1.25, the stores below it are `small` and those above it `large`, and `size_by_count_reason` says `split 8.1k -> 10.2k (x1.26)`. Otherwise `size_by_count` is NULL and the reason says `no gap >= x1.25 (largest valid x...)`. A chain with fewer than 6 such stores gets NULL and the reason `fewer than 6 stores to look for a gap`.

**Produce flag.** `store_format_flag` is `produce` for Hazi Hinam stores whose name contains `תוצרת חקלאית` (5 stores on November 18th, 2026: 207, 208, 210, 217, 219). They are flagged only, not removed, and left out of the gap search and the 75th percentile; their `size_by_count` stays NULL (reason `produce store, left out of the gap search`) unless they are suspect. The flag is NULL for every other store.

**Signal 2, location keywords.** Seed `databricks/seed/store_location_keywords.csv` (`keyword`, `location_type`: `industrial`, `mall`, `commercial_center`; edit it to change the list). A keyword matches `store_name` or `address` only as a whole word: not touching a letter or digit on either side, case-insensitive (so `BIG` matches). `location_type` and `location_keyword` hold the winner: industrial before mall before commercial_center, then the longest keyword; other matching types go to `location_other_matches`. The keyword `מול` ("opposite") was removed on November 18th, 2026: it matched street directions, not malls.

**The chain's own label, for comparison only.** `format_label`: Shufersal and Mahsani Ashuk take `SubChainName` from the store's Stores file; Carrefour takes `היפר`, `מרקט` or `סיטי` from `store_name`; Rami Levy is `רמי לוי בשכונה` for store numbers 700 to 799, else `רמי לוי`; other chains NULL. `label_size` is `small` for `שופרסל אקספרס`, `יש Good`, `ישבשכונה`, Carrefour `סיטי` and `רמי לוי בשכונה`, `full` for any other label, NULL without a label.

Result of version 1 on November 17th, 2026 (the old gold table, old rules): no chain had a gap, 9 stores were `suspect` (4 Hazi Hinam produce stores, 5 Rami Levy stores), 70 stores had a location type. Version 2 has not run yet (the daily compute quota was used up on November 18th); its gap results are not measured.

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

https://www.kaggle.com/datasets/erlichsefi/israeli-supermarkets-2024 (CC BY-NC-SA 3.0 IGO, by Sefi Erlich). The backfill plan and its steps: [docs/backfill-plan.md](docs/backfill-plan.md).

**What it is.** One dataset version per upload day (721 by 2026-11-12), each a full snapshot. About 358 files, 13.9 GB per version: one folder per chain from about version 400 on (older versions are flat), with `price_full_file_<chain>.csv`, `price_file_` (deltas), `promo_full_file_`, `promo_file_`, `store_file_` and JSON status files, plus dataset-level `index.json` (version to date) and quality files. Columns are the regulation's field names.

**How to download one file, no Kaggle login:** `GET https://www.kaggle.com/api/v1/datasets/download/erlichsefi/israeli-supermarkets-2024/<path>?datasetVersionNumber=<N>`, with the `/` of a path like `keshet/price_full_file_keshet.csv` written as `%2F` (a plain `/` answers 404). The answer is a 302 to a signed Google Storage link (range requests work), the same call `kagglehub` makes. `GET .../api/v1/datasets/list/erlichsefi/israeli-supermarkets-2024?datasetVersionNumber=<N>&pageSize=200` lists a version's files with sizes. Lambda `fetch_kaggle` uses only these two calls (Python standard library).

| Fact (sample of 2026-11-12) | What it means for us |
|---|---|
| Same source files as our scraper: same names (ours add `.gz`), 600 of 600 sampled prices equal | Where both exist the rows are identical; "scraper beats Kaggle" is a tie-break, not a conflict |
| All 10 chains covered from 2024-10-22; versions 2-9 (Sept 2024) are broken | The backfill starts at 2024-10-22 |
| Gaps: 2025-01-12..16 (all chains), Hazi Hinam 2024-11-24..12-15, Carrefour holes 2025-05..07; 107 calendar days with no version | History has holes. `price_history` uses `first_seen` / `last_seen`, so a hole is "not observed", never "price changed" |
| A version holds about two days of files; its date is the upload date. 35 dates have two versions | Each row's date comes from its file name; one version per date is loaded (the lowest) |
| The CSV is sparse: a value equal to the row above is left blank, in every column. A block of rows is one source file and starts at a non-empty `file_name` | Bronze fills each blank from the row above, inside the block, in file order (see "Kaggle rows in bronze") |
| The literal `''` means empty in the source | It becomes NULL and is never filled |
| A blank can be "same as above" or "really empty", and the file can't tell | An assumption. No mismatch was found against the XML of the same files |
| Headers are CamelCase in 2024, lowercase later; Keshet and Rami Levy carry two schemas (`priceupdatedate` / `priceupdatetime`, `manufacturername` / `manufacturename`, `itemname` / `itemnm`); one older feed writes `unitmeasure` | One column-name map in the loader |
| Store ids are padded (`020`) in price files, unpadded in some 2024 store files | Silver compares store ids as numbers |
| Line ends are mixed (CRLF and LF in one file) | The reader needs `lineSep = LF` or rows are merged |
| Promotion rows hold nested JSON; 2025 versions use Python-style single quotes. In Victory and Mahsani Ashuk's older feed the promotion file's `chainid` is `72906`, not the 13-digit id, and dates are written `2025/12/12` | Parsed as VARIANT after the quotes are fixed; silver turns `/` into `-` before casting. Silver takes the chain id from the original file name when the header's is short |
| Size of the 10 chains, uncompressed: prices about 1.2 GB per version; gzip shrinks prices 4-5x and promotions about 20x; promotion files grew about 5x from Dec 2025 | Raw is stored gzipped; storage stays around $1-2 a month. Slice 00: 4.42 GB downloaded, 0.99 GB in S3 |
| Compute: about 13-20 DBU a day on our own collection; Databricks reads the Tel Aviv bucket from Ohio, so "networking" is a third to half of it; the Free Edition quota is not published | Load in slices, one slice a day, and read `system.billing.usage` the next day |

**Scope (decided 2026-11-12).** One version a week from 2024-10-22 to file dates before 2026-11-09 (price and store files, about 98 versions); every version in the windows 2024-12-04 to 2025-01-09 and 2025-11-23 to 2025-12-29 (Hanukkah 2024 and 2025, starting 3 weeks early so the "price 14 days earlier" exists), also with promotion files (58 versions); 1-2 versions that overlap our own collection, only to reconcile. 156 versions in all, all stores of the 10 chains, only `price_full_file`, `promo_full_file` and `store_file`. Chain names follow `infra/chains.json` (`victory`, `victory_new_source` -> `VICTORY_NEW_SOURCE`; `mahsani_ashuk(_new_source)` -> `MAHSANI_ASHUK_NEW_SOURCE`; `yayno_bitan(_and_carrefour)` -> `YAYNO_BITAN_AND_CARREFOUR`; `_temp` stems are ignored; when a version has both an old and a `_new_source` file, the new one is used). Load order: Hanukkah 2025 window, Hanukkah 2024 window, then weekly, newest to oldest.

**Raw layout.** `s3://supermarket-detective-raw-971422709739/source=kaggle/date=<version date>/chain=<CHAIN>/<original name>.gz`, plus one manifest per copied file at `source=kaggle/_manifest/v<version>/<CHAIN>_<kind>.json` (sizes listed and received, md5, S3 size, status; written last, so a manifest means a complete file). The Lambda checks the received size against the size Kaggle lists, the md5 against Google's header when it is sent, and the S3 size after upload. Slices are `ingestion/fetch_kaggle/slices/NN.json` (`make_slices.py`), started with `aws stepfunctions start-execution` on state machine `supermarket-detective-backfill` (5 files at a time, 3 retries with a growing wait; it writes no marker, so it never starts `daily_load`).

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

- `ingestion/` - `fetch_chain/` (Lambda container: scraper + upload), `fetch_kaggle/` (Lambda zip + `make_slices.py` + slice files for the Kaggle backfill), `reach_test/`
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
| 3 | History backfill | Kaggle snapshots (weekly back to 2024-10-22, daily around Hanukkah 2024 and 2025, all stores) in the same S3 bucket under `source=kaggle/`, through the same bronze/silver; steps in [docs/backfill-plan.md](docs/backfill-plan.md) | ~2026-11-21 |
| 4 | Gold + Power BI v1 | Price index and the Eilat VAT check; first X post | ~2026-11-28 |
| 5 | Hanukkah index | Holiday prices 2026 vs. Hanukkah 2025 (2025-12-14 to 12-22) | before 2026-12-04 |
| 6 | Extras | Fake promotions, shrinkflation, pipeline health page, n8n + LLM alert to Telegram | December |

Why daily collection comes before history: the chain sites keep only recent files, so every day without the daily run is lost raw data, while Kaggle's archive stays online. The backfill reuses the daily path (same bucket, layout and loading), and which stores to backfill is decided while building silver.

Milestone 1 is built in eight pieces, each working before the next: (1) Terraform setup + S3 raw bucket, (2) ECR repository, (3) `fetch_chain` Python code, (4) Lambda + IAM role, (5) Step Functions Map over the chain list, (6) EventBridge Scheduler at 09:00 (first 07:00, moved because the scraper disables two chains until 08:00), (7) Databricks read access to S3 (first planned as a push to a volume), (8) failure alarm by email.

Milestone 0 results (2026-11-03): all seven geo-blocked chains answered a Lambda in il-central-1. A 32.8 MB Kaggle price file uploaded to the volume in 7 s and read with SQL; the Kaggle CSVs need `multiLine => true, escape => '"'`, otherwise line breaks inside product names split rows (163,660 rows and 41 "stores" instead of 163,176 and 13), which becomes the first reconciliation check. Power BI Desktop imported a test table from the warehouse (Databricks connector, Import mode).
