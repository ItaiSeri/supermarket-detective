# Milestone 3: Kaggle history backfill, the plan

Written 2026-11-12, after step M3a (Airtable row 54). Scope agreed by Itai the same day.

## 1. What and why, in one paragraph

The chain sites keep only a few days of files, so our own collection (since 2026-11-09) has no history. The Kaggle dataset `erlichsefi/israeli-supermarkets-2024` has a daily snapshot of the same chains since 2024. We copy a **sample** of it into our raw bucket and push it through the **same bronze and silver tables**, marked `source = 'kaggle'`. With history we can do: the fake-promotion check (promo price ≥ the regular price 14 days earlier), shrinkflation over two years, and Hanukkah 2024 vs 2025 vs 2026.

## 2. What we learned in M3a (facts the plan relies on)

| Fact | What it means for us |
|---|---|
| Kaggle has **the same source files** as our scraper: same file names, 600/600 sampled prices equal | Where both exist, the rows are identical. "Scraper beats Kaggle" is a tie-break, not a conflict. |
| All 10 chains covered from **2024-10-22**; versions 2-9 (Sept 2024) are broken | Start at 2024-10-22. |
| Gaps: 2025-01-12..16 (all chains), Hazi Hinam 2024-11-24..12-15, Carrefour holes 2025-05..07; 107 calendar days with no version | History has holes. `price_history` uses `first_seen`/`last_seen`, so a hole is "not observed", never "price changed". |
| A version holds **about 2 days of files**; the version date is the upload date, not the file date | Take the date from each file name (silver already does). |
| The CSV is **sparse**: a value equal to the row above is left blank, in every column. A block of rows = one source file and starts at a non-empty `file_name` | Bronze must fill each blank from the row above, inside each block, in the original row order. Tested: this restores the data exactly. |
| The literal `''` means empty in the source; it is not a blank | `''` → NULL, and is never filled from the row above. |
| A blank can be "same as above" or "really empty"; the file can't tell | Assumption, written in the README. |
| Headers are CamelCase in 2024 (`ChainId`), lowercase later; Keshet carries two schemas (`priceupdatedate`/`priceupdatetime`, `manufacturername`/`manufacturename`) | One column-name map in the loader. |
| Store ids are padded (`020`) in price files, unpadded in some 2024 store files | Silver already compares store ids as numbers. |
| Promotions: one row per promotion, items in nested JSON (`groups` or `promotionitems`), `NO_BODY` = empty, a single item can be an object instead of a list | The loader explodes it to the same shape as our scraper's promotion rows. |
| Size (10 chains, uncompressed): price files ~1.2 GB per version; gzip shrinks prices 4-5x, promos ~20x; promo files grew ~5x from Dec 2025 | Raw is stored gzipped. Storage stays around $1-2 a month. |
| Compute: today ~13-20 DBU a day; Databricks reads our Tel Aviv bucket from Ohio, so "networking" is a third to half of it; the Free Edition quota is not published | Load in **slices**, one slice a day, and check `system.billing.usage` the next day. |

## 3. Scope (decided 2026-11-12)

- **Weekly:** one Kaggle version per week, from 2024-10-22 up to file dates before 2026-11-09. About 107 versions. Price + store files only.
- **Daily windows:** every version in these windows, with price + store + **promotion** files:
  - Hanukkah 2024 (2024-12-25 to 2025-01-02): window **2024-12-04 to 2025-01-09**
  - Hanukkah 2025 (2025-12-14 to 12-22): window **2025-11-23 to 2025-12-29**
  - Each window starts 3 weeks early, so the 14-days-earlier price for the fake-promotion check exists.
- **Reconciliation:** 1-2 versions that overlap our own collection (file dates 2026-11-09 or later), loaded only to compare with the scraper.
- **Only full files:** `price_full_file_*`, `promo_full_file_*`, `store_file_*`. Delta files (`price_file_*`, `promo_file_*`) and JSON status files are skipped.
- **All stores** of the 10 chains. If M3b shows that's too expensive, cut to every other week before cutting stores.
- In total about 165 versions. Load order: **Hanukkah 2025 window → Hanukkah 2024 window → weekly, newest to oldest.** Hanukkah first, because the Hanukkah post is due before 2026-12-04.

Kaggle chain names → ours: shufersal, rami_levy, yayno_bitan (older) / yayno_bitan_and_carrefour (newer) → YAYNO_BITAN_AND_CARREFOUR, yohananof, osher_ad, victory / victory_new_source → VICTORY_NEW_SOURCE, hazi_hinam, mahsani_ashuk / mahsani_ashuk_new_source → MAHSANI_ASHUK_NEW_SOURCE, tiv_taam, keshet. Files with a `_temp` stem are ignored. Use the same chain names as `infra/chains.json`.

## 4. Design

```
Kaggle (public, no login)
  -> AWS Lambda fetch_kaggle (one item = one version + chain + file kind)
       download -> gzip -> S3
  -> s3://supermarket-detective-raw-971422709739/source=kaggle/date=<version date>/chain=<CHAIN>/<original name>.gz
       (+ source=kaggle/_manifest/<version>.json: files, bytes, status)
  -> Databricks job kaggle_backfill (started by hand, one slice at a time)
       bronze: load_kaggle_prices, load_kaggle_stores, load_kaggle_promotions
               -> the SAME bronze tables, source = 'kaggle'
       silver: the existing silver_load tasks (they read only new bronze rows)
       ops:    log_run
```

- **Raw layout:** the same as the scraper's. `date=` is Kaggle's version date (its "collection date"), the same meaning `date=` has for the scraper. The file keeps its original Kaggle name plus `.gz`.
- **Download in AWS, not on the laptop:** about 165 versions × ~1.5 GB doesn't belong on a laptop, and nothing scheduled runs there. Kaggle doesn't need an Israeli IP, but the bucket and roles are already in il-central-1.
- **New Lambda `supermarket-detective-fetch-kaggle`:** a small zip Lambda (Python, `requests` + `boto3`), not the scraper image. Timeout 900 s, 1024 MB, `/tmp` 4096 MB. IAM: logs + `s3:PutObject` on `source=kaggle/*` only. The input is one item `{version, chain, kaggle_path, kind}`. It downloads to `/tmp`, gzips while uploading, and checks the downloaded size against the size the Kaggle files API listed.
- **New state machine `supermarket-detective-backfill`:** a Map over the items of one slice, MaxConcurrency 5, retry with a wait (Kaggle rate limits). No schedule: it is started by hand per slice. It does **not** write a `_markers/` file, so it never triggers `daily_load`.
- **Slice list:** a script `ingestion/fetch_kaggle/make_slices.py` builds `ingestion/fetch_kaggle/slices/<NN>.json` from the Kaggle files API: one JSON per slice, already in Step Functions input form `{"items":[...]}`. The slice size comes from the M3b measurement.
- **Bronze Kaggle loader:** a new notebook `bronze_kaggle.py`. It reads the gzipped CSVs with **Spark's own CSV reader** (`multiLine`, `escape '"'`, all columns as strings), not a Python UDF, because the UDF parser runs at ~2M rows a minute and the quota is the constraint. A gzipped file can't be split, so one file = one partition and the original row order survives. Then: row number per file → block id = running count of non-empty `file_name` → fill each blank with `last(col, ignorenulls)` over the block → `''` → NULL → rename columns to the bronze names → one row per product, the same columns as the scraper rows. **Prove the order assumption:** for the M3b files, the Spark result must equal the pandas forward-fill result row for row.
- **Provenance:** in bronze, `file_path` = the S3 path of the Kaggle CSV, and the source file name from the block goes in the column bronze already uses for the source file name (if there is none, add `source_file_name` and update the README). Per-file checks group by source file name.
- **Promotions:** explode `groups` / `promotionitems` into the row shape the scraper loader writes to `bronze.promotion_items`. `NO_BODY` → NULL; handle an object where a list is expected.
- **Stores:** Kaggle store files go into `bronze.stores` only. `silver.stores` stays as the current list (prices from stores missing there are already kept).
- **Silver:** no new tables. Check that each silver notebook handles `source = 'kaggle'` rows: the file date from the name, the scraper-beats-Kaggle rule in the MERGE, and DQ checks that must not FAIL on old dates (completeness is about today's collection, so DQ must look only at scraper rows or the latest date).
- **`price_history`** is rebuilt in full from `price_obs` today. With ~1B Kaggle rows that rebuild gets heavy; measure it in M3b. If it's too slow, switch it to an incremental MERGE (the README already says it must when `price_obs` is trimmed).
- **Never at the same time as `daily_load`:** run the backfill job only after the morning run has finished (about 10:30 onward), because of the 5-concurrent-task limit and to keep each day's compute visible. Its `max_concurrent_runs` is 1.

## 5. Steps

| Step | What | Done when |
|---|---|---|
| **M3b-0** | Daily collection: skip Saturdays. `infra/scheduler.tf` cron `cron(0 9 ? * SUN-FRI *)`. Sunday's run still picks up files the chains published on Saturday, if they're still listed. | `terraform apply` shows 1 change; `aws scheduler get-schedule` shows the new expression |
| **M3b-1** | README "The Kaggle dataset" rewritten with section 2 facts + the scope; this plan linked from the README Plan | committed |
| **M3b-2** | `fetch_kaggle` Lambda + `backfill` state machine in Terraform; `make_slices.py`; slice 00 = one Hanukkah 2025 version (2025-12-15) + one overlap version | applied; slice 00 files in S3 with sizes matching Kaggle's |
| **M3b-3** | `bronze_kaggle.py` + job `kaggle_backfill` in the bundle; run it on slice 00 | bronze rows per source file = rows in that Kaggle block; Spark fill = pandas fill |
| **M3b-4** | Silver over slice 00 + reconciliation: for the overlap version, per source file, row count and price sum equal the scraper's | DQ has no new FAIL; the reconciliation check passes or differences are explained (e.g. later Hazi files) |
| **M3b-5** | Measure: wall time of each task, rows, and the DBUs of that day from `system.billing.usage` (lags a few hours, so the next day) | numbers in the report → slice size chosen |
| **Gate** | Claude verifies, Itai decides the slice size and whether to cut scope | |
| M3c | One slice a day, in the load order above: `aws stepfunctions start-execution` with the slice, then `databricks bundle run kaggle_backfill`; check billing the next day | all slices loaded |
| M3d | Final checks: rows per chain per month, the gaps listed in section 2 show as gaps, `price_history` sane; README + architecture page updated | Airtable rows logged |

## 6. Rules while building

- Work on branch `m3-kaggle-backfill`. Databricks deploys and Terraform applies run **only from `main`**, one at a time: merge to local `main` first, then deploy or apply. Never push (Itai pushes).
- A change to a data contract (S3 layout, table columns) updates the README and `docs/architecture.html` in the same commit.
- `infra/terraform.tfvars` is git-ignored and holds the alarm email; never commit it.
- Terraform runs in PowerShell, not Git Bash. The Databricks CLI lives in `%LOCALAPPDATA%\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_...` (profile `supermarket`).
- Read-only checks are always fine. Nothing that deletes data in S3 or drops a table.

## 7. Interview points

- Why sample (weekly + windows) instead of every day: the quota, and weekly is enough for slow-changing prices; daily only where the question needs it.
- Why slices with a manifest: a restartable backfill, the same idea as a batch restart point.
- Why Spark's CSV reader and not a UDF: cost. The order-preservation assumption is proven, not assumed.
- In production you'd pay for compute and load it in one go, and keep raw in the same region as compute to avoid networking cost.

## 8. Change 2026-11-12 afternoon: storage next to compute (Ohio) + a batch queue

**Why:** slice 00's bill showed cross-region networking (Databricks in Ohio reading our Tel Aviv bucket) at ~15 DBU against ~3 DBU of real compute. Every read of bronze crossed the ocean, because bronze was an external table in the Tel Aviv bucket. Decided by Itai 2026-11-12: move everything Databricks reads repeatedly to Ohio (us-east-2); only the scraper must stay in Tel Aviv (Israeli IP). Itai also decided: run the whole backfill now, unattended, in batches with a log table; hitting the Free Edition quota is acceptable (work resumes when it resets), and a missed daily run is acceptable.

**Target layout:**
```
SCRAPER: chain sites -> Lambda (Tel Aviv) -> raw S3 Tel Aviv (unchanged) --+  read once by Auto Loader
                                                                           +--> BRONZE managed (Ohio) -> SILVER (Ohio) -> GOLD
KAGGLE:  Kaggle (Google Cloud, USA) -> Lambda (Ohio) -> raw S3 Ohio -------+  no ocean crossing
```
- **New bucket** `supermarket-detective-kaggle-971422709739` in us-east-2: versioning, public access blocked, Intelligent-Tiering, layout `source=kaggle/date=<version date>/chain=<CHAIN>/<name>.gz` + `_manifest/` as before. Read-only external location for Databricks.
- **Kaggle Lambda + backfill state machine move to us-east-2** (same code). Downloading all ~165 versions up front is cheap (Lambda + ~50 GB S3), so the download is separate from loading.
- **Bronze becomes managed Delta tables** (Databricks storage, Ohio), same names and columns, via a one-time `DEEP CLONE`; Auto Loader checkpoints kept, so no file loads twice. Bronze stays rebuildable from raw, which stays in our S3. Kaggle rows go into the same bronze tables with `source='kaggle'`.
- The Tel Aviv `bronze/` prefix is left in place (not deleted) until Itai decides.
- The daily DQ must not scan all of bronze; reconciliation checks look only at the newest files.

**Batch queue (resumable backfill):**
- `workspace.ops.backfill_queue`: one row per Kaggle version in load order (Hanukkah 2025 window, Hanukkah 2024 window, then weekly newest → oldest), with `status` (`pending` / `running` / `done` / `failed`), `batch_id`, `started_at`, `ended_at`, `job_run_id`, rows added per table, `error`.
- Job `kaggle_backfill`, one run = one batch: claim the next N `pending` versions (or a `running`/`failed` batch left over from an interrupted run) → delete any bronze rows of those files (makes a retry safe) → load their files into bronze → run the silver tasks → mark `done` with row counts. Silver MERGEs are already safe to repeat.
- Scheduled every 30 minutes, `max_concurrent_runs` 1, until the queue is empty; then the schedule is paused. If the quota stops compute, the batch stays `running`/`failed` and the next run after the reset redoes it. The queue table is the "where did we stop" answer.

## 9. Change 2026-11-12 evening: store-wide promotions

Batch 2 of the queue failed after 63 minutes because a few promotions attach the whole catalog to a basket-level offer (Carrefour December 2025: about 34,000 products each, in about 170 store files, 17 of 20.9 million promotion rows a day). Decided by Itai: a promotion that lists more than 10,000 products in one source file is stored as ONE bronze row (`store_wide`, `items_in_promotion`) and one row in `silver.store_wide_promotions`; see the README. All Kaggle promotion rows were deleted and batches 1 and 2 reset to `pending`; the schedule was paused during the change.
