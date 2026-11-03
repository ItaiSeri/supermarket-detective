"""Day-1 reach test: can a Lambda in AWS Tel Aviv download each chain's files?

Invoke with {"chains": ["SHUFERSAL", ...], "file_type": "STORE_FILE"}.
Returns the Lambda's public IP location and, per chain, whether one file came down.
Throwaway: delete the function and its ECR repo once the answer is in.
"""

import os

# Lambda's code dir is read-only; the scraper writes status files relative to cwd.
os.chdir("/tmp")

import asyncio
import glob
import time

import requests
from il_supermarket_scarper import ScraperFactory
from il_supermarket_scarper.utils import DiskFileOutput

DEFAULT_CHAINS = [
    # geo-blocked outside Israel as of 2026-11-02
    "HAZI_HINAM",
    "HET_COHEN_NEW_SOURCE",
    "MAHSANI_ASHUK_NEW_SOURCE",
    "NETIV_HASED",
    "SUPER_PHARM",
    "VICTORY_NEW_SOURCE",
    "YAYNO_BITAN_AND_CARREFOUR",
    # controls: reachable from anywhere
    "SHUFERSAL",
    "RAMI_LEVY",
]
PER_CHAIN_TIMEOUT_S = 240


def location():
    try:
        ip = requests.get("https://api.ipify.org?format=json", timeout=10).json()["ip"]
        geo = requests.get(f"https://ipinfo.io/{ip}/json", timeout=10).json()
        return {"ip": ip, "city": geo.get("city"), "country": geo.get("country"), "org": geo.get("org")}
    except Exception as e:  # location is nice to have, not the test
        return {"error": f"{type(e).__name__}: {e}"}


async def drain(scraper, file_type):
    # scrape() is an async generator in 1.0.19: it yields each downloaded file.
    async for _ in scraper.scrape(limit=1, files_types=[file_type]):
        pass


def try_chain(name, file_type):
    started = time.time()
    folder = f"/tmp/reach/{name}"
    try:
        scraper = ScraperFactory.get(name)(file_output=DiskFileOutput(storage_path=folder))
        asyncio.run(asyncio.wait_for(drain(scraper, file_type), timeout=PER_CHAIN_TIMEOUT_S))
        files = [
            f for f in glob.glob(f"{folder}/**/*", recursive=True)
            if os.path.isfile(f) and not f.endswith(".json")
        ]
        return {
            "chain": name,
            "ok": bool(files),
            "files": [os.path.basename(f) for f in files][:3],
            "bytes": sum(os.path.getsize(f) for f in files),
            "secs": round(time.time() - started, 1),
        }
    except Exception as e:
        return {
            "chain": name,
            "ok": False,
            "error": f"{type(e).__name__}: {str(e)[:300]}",
            "secs": round(time.time() - started, 1),
        }


def handler(event, context):
    event = event or {}
    chains = event.get("chains") or DEFAULT_CHAINS
    file_type = event.get("file_type", "STORE_FILE")
    return {
        "location": location(),
        "results": [try_chain(name, file_type) for name in chains],
    }
