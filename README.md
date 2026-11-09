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
             PriceFull/PromoFull/Stores          | field mapping (chain_config), types     Eilat check, fake promos,
          b) or kagglehub: that day's            v                                         shrinkflation,
             Kaggle file (geo-blocked)    SILVER stores, items (barcode master),          pipeline health
          -> S3 raw bucket (versioned)           price_obs, price_history (SCD2),
          -> manifest row                        promotions + promo_items, quarantine
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
- **One parser, driven by config.** A `chain_config` table maps each chain's layout to one schema. Adding a chain is a new row, not new code. Same idea as the partition-wise PL/SQL framework at Mizrahi.
- **Price history is SCD Type 2,** keyed on chain + store + barcode: a new row only when the price changes, with `valid_from` / `valid_to`. Daily full files make this the "source sends a full dump" CDC problem.
- **Data quality at three levels:** file (expected chain/store/type arrived; row count within ±30% of the 7-day average), row (valid barcode, price > 0, parsable quantity; failures go to quarantine), business (price jump > 300% flagged).
- **Stay under the daily compute quota:** sampled stores (every Eilat branch + about 3 central branches per chain + the online store), new files only, one pipeline.
- **Everything as code:** AWS resources in Terraform or CDK, Databricks pipelines and jobs in a bundle, all in GitHub with CI. Free Edition accounts can be deleted after long inactivity, so the repo must rebuild everything.

### Bronze table: `workspace.bronze.price_items`

External Delta table in `s3://supermarket-detective-raw-971422709739/bronze/price_items`, created by `databricks/notebooks/bronze_price_items.py`. One row per product in a PriceFull file. Stores and promotions get their own bronze tables later, because their shapes differ.

| Column | Type | Meaning |
|---|---|---|
| `file_path` | STRING | Full S3 path of the source file |
| `file_name` | STRING | File name; silver takes the file's date from it |
| `source` | STRING | `scraper` or `kaggle` |
| `chain` | STRING | Chain, from the S3 folder |
| `collected_date` | DATE | Collection date, from the S3 folder |
| `modified_at` | TIMESTAMP | When the file landed in S3 |
| `declared_count` | STRING | `Count` attribute of `<Items>`; found only in Keshet's sample, otherwise NULL |
| `header` | MAP<STRING, STRING> | The file's top-level fields, such as `StoreId` |
| `fields` | MAP<STRING, STRING> | This product's fields, as published |
| `loaded_at` | TIMESTAMP | When the row was loaded |

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
- `databricks/` - bundle: pipeline (bronze/silver/gold), jobs, `chain_config`
- `powerbi/` - .pbix and screenshots for X posts
- `data/` - local scratch only (git-ignored)

## Plan

| # | Milestone | What exists at the end | Target |
|---|---|---|---|
| 0 | Prove the risky parts | AWS Tel Aviv reaches every chain, Databricks accepts uploads, Power BI connects to the Free Edition warehouse | Done 2026-11-03 |
| 1 | Daily collection in the cloud | Every morning at 09:00 AWS downloads all chains' files into S3, where Databricks can read them directly, with no laptop involved | ~2026-11-10 |
| 2 | Bronze + silver | Raw files become clean tables (stores, products, prices) with SCD2 price history, quality checks, quarantine and ops tables; two chains first, then the rest via `chain_config` | ~2026-11-17 |
| 3 | History backfill | Kaggle snapshots (weekly back to 2024-09-22, daily for the last 60 days and around Hanukkah 2025) in the same S3 bucket under `source=kaggle/`, sampled stores only, through the same bronze/silver | ~2026-11-21 |
| 4 | Gold + Power BI v1 | Price index and the Eilat VAT check; first X post | ~2026-11-28 |
| 5 | Hanukkah index | Holiday prices 2026 vs. Hanukkah 2025 (2025-12-14 to 12-22) | before 2026-12-04 |
| 6 | Extras | Fake promotions, shrinkflation, pipeline health page, n8n + LLM alert to Telegram | December |

Why daily collection comes before history: the chain sites keep only recent files, so every day without the daily run is lost raw data, while Kaggle's archive stays online. The backfill reuses the daily path (same bucket, layout and loading), and which stores to backfill is decided while building silver.

Milestone 1 is built in eight pieces, each working before the next: (1) Terraform setup + S3 raw bucket, (2) ECR repository, (3) `fetch_chain` Python code, (4) Lambda + IAM role, (5) Step Functions Map over the chain list, (6) EventBridge Scheduler at 09:00 (first 07:00, moved because the scraper disables two chains until 08:00), (7) Databricks read access to S3 (first planned as a push to a volume), (8) failure alarm by email.

Milestone 0 results (2026-11-03): all seven geo-blocked chains answered a Lambda in il-central-1. A 32.8 MB Kaggle price file uploaded to the volume in 7 s and read with SQL; the Kaggle CSVs need `multiLine => true, escape => '"'`, otherwise line breaks inside product names split rows (163,660 rows and 41 "stores" instead of 163,176 and 13), which becomes the first reconciliation check. Power BI Desktop imported a test table from the warehouse (Databricks connector, Import mode).
