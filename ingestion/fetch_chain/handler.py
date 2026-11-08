"""fetch_chain: download one chain's files for today and upload them to the raw S3 bucket.

Event: {"chain": "RAMI_LEVY", "file_types": ["STORE_FILE"], "limit": 1, "store_id": null}
"""

import asyncio
import gzip
import os
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# the scraper opens logging.log in the current folder as soon as it is imported;
# in Lambda the code folder (/var/task) is read-only, only /tmp is writable
os.chdir(tempfile.gettempdir())

import boto3
from il_supermarket_scarper import ScraperFactory
from il_supermarket_scarper.utils import DiskFileOutput

BUCKET = os.environ.get("RAW_BUCKET", "supermarket-detective-raw-971422709739")
DEFAULT_FILE_TYPES = ["STORE_FILE", "PRICE_FULL_FILE", "PROMO_FULL_FILE"]

s3 = boto3.client("s3")


def ship(path, prefix):
    """Zip one downloaded file, upload it to S3, delete the local copies. Returns (key, size)."""
    # the scraper unzips the chain's .gz on download; zip it back so S3 stores ~20x less
    if path.suffix == ".gz":
        gz_path = path  # already compressed: upload as is
    else:
        gz_path = path.with_name(path.name + ".gz")
        with open(path, "rb") as src, gzip.open(gz_path, "wb") as dst:
            shutil.copyfileobj(src, dst)  # streams in chunks, not the whole file in memory
    key = f"{prefix}/{gz_path.name}"
    s3.upload_file(str(gz_path), BUCKET, key)
    size = gz_path.stat().st_size
    path.unlink(missing_ok=True)
    gz_path.unlink(missing_ok=True)  # a full chain is more than /tmp can hold at once
    return key, size


async def download_and_ship(chain, folder, file_types, limit, store_id, prefix, uploaded, failures):
    scraper = ScraperFactory.get(chain)(file_output=DiskFileOutput(storage_path=folder))
    # scrape() is an async generator: it yields once per finished file, at most 10 in flight.
    # It starts no new downloads while this loop body runs, so /tmp stays small.
    async for result in scraper.scrape(limit=limit, files_types=file_types, store_id=store_id):
        if result.error or not result.extract_succefully:  # sic: the library's spelling
            failures.append(f"{result.file_name}: {result.error}"[:200])
            continue
        path = Path(folder) / result.file_name
        if path.exists():  # missing means the scraper skipped it as identical content
            key, size = await asyncio.to_thread(ship, path, prefix)
            uploaded[key] = size


def handler(event, context=None):
    started = time.time()
    chain = event["chain"]
    file_types = event.get("file_types", DEFAULT_FILE_TYPES)
    limit = event.get("limit")
    store_id = event.get("store_id")

    run_date = datetime.now(ZoneInfo("Asia/Jerusalem")).strftime("%Y-%m-%d")
    prefix = f"source=scraper/date={run_date}/chain={chain}"

    error = None
    uploaded = {}  # S3 key -> size; a file listed twice overwrites its own key
    failures = []
    old_cwd = os.getcwd()

    with tempfile.TemporaryDirectory() as tmp:
        # the scraper writes status files to the current folder; in Lambda only /tmp is writable
        os.chdir(tmp)
        try:
            folder = Path(tmp) / "files"
            try:
                asyncio.run(
                    download_and_ship(
                        chain, str(folder), file_types, limit, store_id, prefix, uploaded, failures
                    )
                )
            except Exception as e:
                error = f"{type(e).__name__}: {e}"[:300]
        finally:
            os.chdir(old_cwd)  # Windows can't delete a folder you're standing in

    # the scraper swallows site timeouts, so zero files is the only sign of a dead site;
    # raising makes the run a real failure that Step Functions can retry
    if not uploaded:
        reason = error or (failures[0] if failures else "the site returned no files")
        raise RuntimeError(f"{chain}: nothing uploaded ({reason})")

    return {
        "chain": chain,
        "date": run_date,
        "ok": error is None and not failures,
        "files": len(uploaded),
        "failed": len(failures),
        "bytes": sum(uploaded.values()),
        "s3_prefix": f"s3://{BUCKET}/{prefix}/",
        "error": error or (failures[0] if failures else None),
        "secs": round(time.time() - started, 1),
    }


if __name__ == "__main__":
    import json
    import sys

    chain = sys.argv[1] if len(sys.argv) > 1 else "RAMI_LEVY"
    test_event = {"chain": chain, "file_types": ["STORE_FILE"], "limit": 1}
    print(json.dumps(handler(test_event), indent=2, ensure_ascii=False))