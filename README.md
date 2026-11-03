# Supermarket Detective

Israeli supermarket prices as a lakehouse: every chain's legally required price and promo files, kept as full price history, with a Power BI report on top.
Questions it answers: is Eilat really 15.3% cheaper (no VAT)? Which "promotions" are not cheaper than the regular price was? Which products shrank while the price stayed? How do prices move before holidays?

Stack: AWS (Tel Aviv region, ingestion) → Databricks Free Edition (Unity Catalog, Auto Loader, Lakeflow declarative pipelines, Jobs, bundles) → Power BI. Everything scheduled runs in the cloud; the laptop is only for writing code and building the Power BI report.

## Architecture

```
AWS il-central-1 (Tel Aviv)               DATABRICKS FREE EDITION                        POWER BI
---------------------------               ------------------------------------------     ---------------
EventBridge Scheduler (daily 07:00)       Volume /Volumes/<cat>/landing/raw/             Desktop (authoring),
  -> Step Functions state machine           source=scraper|kaggle/date=/chain=/          Import from the SQL
     Map state, one branch per chain        manifest/                                     warehouse
     (from chain_list.json), retries             |                                       star schema over gold:
     per chain                                   | Auto Loader (new files only)            fact_price_daily,
       -> Lambda fetch_chain (container)         v                                         dim_product, dim_store,
          a) il-supermarket-scraper:      BRONZE raw_files: one row per file               dim_chain, dim_date
             raw XML, sampled store ids,         (path, source, chain, payload, metadata)  pages: price index,
             PriceFull/PromoFull/Stores          | config-driven parse (chain_config)      Eilat check, fake promos,
          b) or kagglehub: that day's            v                                         shrinkflation,
             Kaggle file (geo-blocked)    SILVER stores, items (barcode master),          pipeline health
          -> S3 raw bucket (versioned)           price_obs, price_history (SCD2),
          -> manifest row                        promotions + promo_items, quarantine
  -> Lambda push_to_databricks                   |
     (Files API upload of the day's files)       v
  -> CloudWatch alarm on a failed run     GOLD   price_index_daily (fixed basket), eilat_vs_center,
                                                 fake_promo_candidates, shrinkflation_candidates
One-off backfill: same Lambda, Map over
 sampled Kaggle versions (2024-09-22 on)
                                          OPS    run_log, dq_results, source_completeness
```

### Design decisions

- **Ingestion lives in AWS, not in Databricks.** Free Edition only reaches a short list of trusted domains (LinkedIn verification may widen it, still to test). The Tel Aviv region also gives an Israeli IP for chains that block foreign traffic; whether they accept AWS addresses is the first thing to test.
- **Lambda per chain, orchestrated by Step Functions.** The scraper filters by store id and file type, so one chain's sampled stores fit easily in one standard Lambda run (15-minute limit, up to 10 GB memory and /tmp). The Map state runs chains in parallel and retries each one alone, so one broken chain site doesn't fail the whole day. The scraper's headless browser (Playwright) is only a fallback for reading the chain list from gov.il, not for downloading files; the container image includes it anyway. Fargate is the fallback if a chain ever needs longer than 15 minutes.
  - Considered and skipped: Lambda Managed Instances (90-minute timeout since 2026-09) run on EC2 capacity billed even when idle, so they cost more than this daily batch needs. Lambda durable functions (announced for Tel Aviv in 2026-04) could replace Step Functions as the orchestrator; Step Functions gives a visual run graph, which is better for a portfolio.
- **Two sources, one bronze.** Our own scraper keeps the raw XML (the real integration work); the Kaggle daily snapshot supplies history and the geo-blocked chains. Both land in the same raw layout tagged with `source`, and on days both cover a chain, a reconciliation check compares them.
- **S3 is the archive of record.** Raw files are kept forever in a versioned bucket; Databricks can be rebuilt from it.
- **Bronze keeps files exactly as published.** Parsing happens in silver, so a parser bug never loses data: fix it and re-run silver.
- **One parser, driven by config.** A `chain_config` table maps each chain's layout to one schema. Adding a chain is a new row, not new code. Same idea as the partition-wise PL/SQL framework at Mizrahi.
- **Price history is SCD Type 2,** keyed on chain + store + barcode: a new row only when the price changes, with `valid_from` / `valid_to`. Daily full files make this the "source sends a full dump" CDC problem.
- **Data quality at three levels:** file (expected chain/store/type arrived; row count within ±30% of the 7-day average), row (valid barcode, price > 0, parsable quantity; failures go to quarantine), business (price jump > 300% flagged).
- **Stay under the daily compute quota:** sampled stores (every Eilat branch + about 3 central branches per chain + the online store), new files only, one pipeline.
- **Everything as code:** AWS resources in Terraform or CDK, Databricks pipelines and jobs in a bundle, all in GitHub with CI. Free Edition accounts can be deleted after long inactivity, so the repo must rebuild everything.

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

1. **Day 1 - prove the risky parts.** ~~Run the scraper once in AWS Tel Aviv: are the seven blocked chains reachable?~~ Done 2026-11-03: yes, all of them. **Upload test done 2026-11-03:** schema `workspace.landing` + managed volume `raw` created; a 32.8 MB Kaggle price file uploaded through the Files API in 7 s and read back with SQL on the 2X-Small warehouse (`a000b0ebdfdcd00b`). Lesson: read the Kaggle CSVs with `multiLine => true, escape => '"'`; without it, line breaks inside product names split rows (163,660 rows and 41 "stores" instead of 163,176 and 13). This becomes the first reconciliation check (row count vs. the file's own count). Still open: sign up for Databricks Free Edition; upload one file to a Volume via the Files API; connect Power BI Desktop to the SQL warehouse (not confirmed for Free Edition; fallback is exporting gold tables to Parquet).
2. **Days 2-4 - daily ingestion in the cloud.** Container image → ECR, Lambda + Step Functions, daily schedule, S3 archive, upload to the Volume, failure alarm. From here history accumulates without the laptop.
3. **Week 2 - bronze + silver** for two chains with different layouts, then the rest via `chain_config`. SCD2 history, quality checks, ops tables.
4. **Week 2-3 - backfill** from Kaggle: weekly snapshots back to 2024-09-22 plus daily for the last 60 days, sampled stores only.
5. **Week 3 - gold + Power BI v1.** First X post: the Eilat VAT check.
6. **Before 2026-12-04 - the Hanukkah index,** comparing with Hanukkah 2025 (2025-12-14 to 12-22) from the backfill. Then fake promotions and shrinkflation.
7. Optional: n8n + LLM alert to Telegram when a quality check fails.
