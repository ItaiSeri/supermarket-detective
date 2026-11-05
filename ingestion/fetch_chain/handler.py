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

import boto3
from il_supermarket_scarper import ScraperFactory
from il_supermarket_scarper.utils import DiskFileOutput

BUCKET = os.environ.get("RAW_BUCKET", "supermarket-detective-raw-971422709739")
DEFAULT_FILE_TYPES = ["STORE_FILE", "PRICE_FULL_FILE", "PROMO_FULL_FILE"]

s3 = boto3.client("s3")


async def download(chain, folder, file_types, limit, store_id):
    scraper = ScraperFactory.get(chain)(file_output=DiskFileOutput(storage_path=folder))
    # scrape() is an async generator: it yields once per downloaded file
    async for _ in scraper.scrape(limit=limit, files_types=file_types, store_id=store_id):
        pass


def handler(event, context=None):
    started = time.time()
    chain = event["chain"]
    file_types = event.get("file_types", DEFAULT_FILE_TYPES)
    limit = event.get("limit")
    store_id = event.get("store_id")

    run_date = datetime.now(ZoneInfo("Asia/Jerusalem")).strftime("%Y-%m-%d")
    prefix = f"source=scraper/date={run_date}/chain={chain}"

    error = None
    uploaded = []
    total_bytes = 0
    old_cwd = os.getcwd()

    with tempfile.TemporaryDirectory() as tmp:
        # the scraper writes status files to the current folder; in Lambda only /tmp is writable
        os.chdir(tmp)
        try:
            folder = Path(tmp) / "files"
            try:
                asyncio.run(download(chain, str(folder), file_types, limit, store_id))
            except Exception as e:
                error = f"{type(e).__name__}: {e}"[:300]

            files = [f for f in folder.rglob("*") if f.is_file() and f.suffix != ".json"]
            for f in files:
                # the scraper unzips the chain's .gz on download; zip it back so S3 stores ~10x less
                if f.suffix == ".gz":
                    gz_path = f  # already compressed: upload as is
                else:
                    gz_path = f.with_name(f.name + ".gz")
                    with open(f, "rb") as src, gzip.open(gz_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)  # streams in chunks, not the whole file in memory
                key = f"{prefix}/{gz_path.name}"
                s3.upload_file(str(gz_path), BUCKET, key)
                uploaded.append(key)
                total_bytes += gz_path.stat().st_size
        finally:
            os.chdir(old_cwd)  # Windows can't delete a folder you're standing in

    return {
        "chain": chain,
        "date": run_date,
        "ok": bool(uploaded) and error is None,
        "files": len(uploaded),
        "bytes": total_bytes,
        "s3_prefix": f"s3://{BUCKET}/{prefix}/",
        "error": error,
        "secs": round(time.time() - started, 1),
    }


if __name__ == "__main__":
    import json
    import sys

    chain = sys.argv[1] if len(sys.argv) > 1 else "RAMI_LEVY"
    test_event = {"chain": chain, "file_types": ["STORE_FILE"], "limit": 1}
    print(json.dumps(handler(test_event), indent=2, ensure_ascii=False))