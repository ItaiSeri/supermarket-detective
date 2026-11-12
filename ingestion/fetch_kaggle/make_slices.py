"""make_slices: build the Step Functions inputs for the Kaggle backfill.

  python make_slices.py slice00 --out slices/00.json
      the test slice: version dated 2025-12-15 (price + store + promo) and one version
      that overlaps our own collection (price + store), see docs/backfill-plan.md
  python make_slices.py plan --per-slice 5 --out-dir slices
      the whole backfill, cut into slices of N versions, in the load order of the plan:
      Hanukkah 2025 window -> Hanukkah 2024 window -> weekly, newest to oldest

Each slice file is {"items": [ {version, date, chain, kind, kaggle_path, size}, ... ]}, the input of
the state machine supermarket-detective-backfill (one Map item = one file to copy).

The file listing comes from Kaggle's public files API (no login). With --cache-dir it reads
v<N>.json listings ([name, bytes, creationDate] rows) saved earlier, which saves ~700 calls.
Standard library only.
"""

import argparse
import datetime as dt
import json
import os
import re
import time
import urllib.parse
import urllib.request

DATASET = "erlichsefi/israeli-supermarkets-2024"
LIST_URL = f"https://www.kaggle.com/api/v1/datasets/list/{DATASET}"
DOWNLOAD_URL = f"https://www.kaggle.com/api/v1/datasets/download/{DATASET}"

# Kaggle's file stem -> our chain name (infra/chains.json). A "_new_source" stem is the chain's
# newer feed; when a version has both, only the newer one is used.
STEMS = {
    "shufersal": "SHUFERSAL",
    "rami_levy": "RAMI_LEVY",
    "yayno_bitan": "YAYNO_BITAN_AND_CARREFOUR",
    "yayno_bitan_and_carrefour": "YAYNO_BITAN_AND_CARREFOUR",
    "yohananof": "YOHANANOF",
    "osher_ad": "OSHER_AD",
    "victory": "VICTORY_NEW_SOURCE",
    "victory_new_source": "VICTORY_NEW_SOURCE",
    "hazi_hinam": "HAZI_HINAM",
    "mahsani_ashuk": "MAHSANI_ASHUK_NEW_SOURCE",
    "mahsani_ashuk_new_source": "MAHSANI_ASHUK_NEW_SOURCE",
    "tiv_taam": "TIV_TAAM",
    "keshet": "KESHET",
}
PREFERRED = ["victory_new_source", "mahsani_ashuk_new_source", "yayno_bitan_and_carrefour"]
KINDS_PRICE_STORE = ["price_full_file", "store_file"]
KINDS_ALL = ["price_full_file", "store_file", "promo_full_file"]
FILE_RE = re.compile(r"^(price_full_file|promo_full_file|store_file)_(.+)\.csv$")


def http_json(url, attempts=6):
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return json.load(r)
        except Exception:
            if attempt == attempts - 1:
                raise
            time.sleep(2**attempt)


def list_version(version, cache_dir=None):
    """[(name, bytes)] of one version; names as Kaggle lists them (flat in old versions, chain/name in new ones)."""
    if cache_dir and os.path.exists(f"{cache_dir}/v{version}.json"):
        return [(n, b) for n, b, *_ in json.load(open(f"{cache_dir}/v{version}.json"))]
    files, token = [], None
    while True:
        q = {"pageSize": 200, "datasetVersionNumber": version}
        if token:
            q["pageToken"] = token
        page = http_json(f"{LIST_URL}?{urllib.parse.urlencode(q)}")
        files += [(f["name"], f["totalBytes"]) for f in page.get("datasetFiles", [])]
        token = page.get("nextPageToken") or page.get("nextPageTokenNullable")
        if not token:
            return files
        time.sleep(0.3)


def version_dates(latest):
    """version -> date, from the dataset's own index.json (D/M/YYYY, optionally with a time)."""
    url = f"{DOWNLOAD_URL}/index.json?datasetVersionNumber={latest}"
    with urllib.request.urlopen(url, timeout=60) as r:
        index = json.load(r)
    out = {}
    for v, s in index.items():
        s = s.strip()
        m = re.match(r"(\d+)/(\d+)/(\d{4})", s)
        out[int(v)] = dt.date(int(m[3]), int(m[2]), int(m[1])) if m else dt.date.fromisoformat(s[:10])
    return out


def items_for(version, date, kinds, cache_dir=None):
    """One item per chain and kind for this version, from the version's file listing."""
    found = {}  # (chain, kind) -> (stem, name, size)
    for name, size in list_version(version, cache_dir):
        m = FILE_RE.match(name.split("/")[-1])
        if not m or m[2] not in STEMS or m[1] not in kinds:
            continue  # delta files, json status files, _temp stems and other chains are skipped
        key = (STEMS[m[2]], m[1])
        old = found.get(key)
        if old is None or (m[2] in PREFERRED and old[0] not in PREFERRED):
            found[key] = (m[2], name, size)
    return [
        {"version": version, "date": date.isoformat(), "chain": chain, "kind": kind,
         "kaggle_path": name, "size": size}
        for (chain, kind), (_, name, size) in sorted(found.items())
    ]


def one_version_per_date(dates):
    """version -> date, keeping the lowest version of each date (the files are written under date=)."""
    first = {}
    for v in sorted(dates):
        first.setdefault(dates[v], v)
    return {v: d for d, v in first.items()}


def write_slice(path, items, note):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"note": note, "items": items}, f, indent=1)
    gb = sum(i["size"] for i in items) / 1e9
    print(f"{path}: {len(items)} items, {len({i['version'] for i in items})} versions, {gb:.2f} GB raw")


def cmd_slice00(args):
    dates = version_dates(args.latest)
    items = items_for(405, dates[405], KINDS_ALL, args.cache_dir)
    # v720: its files are dated 2026-11-10 and 11, days our own collection also covers
    items += items_for(720, dates[720], KINDS_PRICE_STORE, args.cache_dir)
    write_slice(args.out, items,
                "slice 00: Hanukkah 2025 version (price+store+promo) and one overlap version (price+store)")


def cmd_plan(args):
    dates = one_version_per_date(version_dates(args.latest))
    last_file_day = dt.date(2026, 11, 8)  # our own collection starts 2026-11-09
    newest_first = sorted(dates.items(), key=lambda x: x[1], reverse=True)
    h25 = [v for v, d in newest_first if dt.date(2025, 11, 23) <= d <= dt.date(2025, 12, 29)]
    h24 = [v for v, d in newest_first if dt.date(2024, 12, 4) <= d <= dt.date(2025, 1, 9)]
    in_windows = set(h25) | set(h24)
    # weekly: for each week starting 2024-10-22, the first version dated in that week
    weekly, week = [], dt.date(2024, 10, 22)
    by_date = sorted((d, v) for v, d in dates.items())
    while week <= last_file_day:
        pick = next((v for d, v in by_date if week <= d < week + dt.timedelta(days=7)), None)
        if pick and pick not in in_windows and dates[pick] <= last_file_day:
            weekly.append(pick)
        week += dt.timedelta(days=7)
    weekly.sort(key=lambda v: dates[v], reverse=True)
    order = [(v, KINDS_ALL) for v in h25 + h24] + [(v, KINDS_PRICE_STORE) for v in weekly]
    print(f"versions: Hanukkah 2025 {len(h25)}, Hanukkah 2024 {len(h24)}, weekly {len(weekly)}, total {len(order)}")
    for n in range(0, len(order), args.per_slice):
        items = []
        for v, kinds in order[n:n + args.per_slice]:
            items += items_for(v, dates[v], kinds, args.cache_dir)
        number = n // args.per_slice + 1
        write_slice(f"{args.out_dir}/{number:02d}.json", items,
                    f"slice {number:02d}: versions {[v for v, _ in order[n:n + args.per_slice]]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--latest", type=int, default=721, help="newest Kaggle version (its index.json holds all dates)")
    ap.add_argument("--cache-dir", help="folder with saved v<N>.json listings")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s0 = sub.add_parser("slice00")
    s0.add_argument("--out", default="slices/00.json")
    s0.set_defaults(fn=cmd_slice00)
    pl = sub.add_parser("plan")
    pl.add_argument("--per-slice", type=int, required=True)
    pl.add_argument("--out-dir", default="slices")
    pl.set_defaults(fn=cmd_plan)
    a = ap.parse_args()
    a.fn(a)
