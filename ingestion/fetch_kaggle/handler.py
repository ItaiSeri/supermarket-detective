"""fetch_kaggle: copy one Kaggle file (one version + chain + file kind) into the raw S3 bucket, gzipped.

Event (one Step Functions Map item, built by make_slices.py):
  {"version": 405, "date": "2025-12-15", "chain": "KESHET", "kind": "price_full_file",
   "kaggle_path": "price_full_file_keshet.csv", "size": 95646683}

Output files:
  s3://<bucket>/source=kaggle/date=<date>/chain=<CHAIN>/<original file name>.gz
  s3://<bucket>/source=kaggle/_manifest/v<version>/<CHAIN>_<kind>.json   (what was copied, sizes, md5, status)

Only the standard library and boto3 (already in the Lambda runtime): no packaging needed.
The download is the same plain HTTP call that kagglehub makes; it needs no Kaggle login.
"""

import gzip
import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64decode

import boto3

BUCKET = os.environ.get("RAW_BUCKET", "supermarket-detective-raw-971422709739")
API = "https://www.kaggle.com/api/v1/datasets/download/erlichsefi/israeli-supermarkets-2024"
CHUNK = 1024 * 1024

s3 = boto3.client("s3")


def kaggle_url(path, version):
    # the slash in a path like keshet/price_full_file_keshet.csv must be %2F, or Kaggle answers 404
    return f"{API}/{urllib.parse.quote(path, safe='')}?datasetVersionNumber={version}"


def open_with_retries(url, attempts=5):
    # Kaggle answers 302 (a signed Google Storage link, followed automatically); 429/5xx are retried
    for attempt in range(attempts):
        try:
            return urllib.request.urlopen(urllib.request.Request(url), timeout=60)
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or attempt == attempts - 1:
                raise
        except urllib.error.URLError:
            if attempt == attempts - 1:
                raise
        time.sleep(5 * 2**attempt)


def manifest_key(event):
    return f"source=kaggle/_manifest/v{int(event['version']):04d}/{event['chain']}_{event['kind']}.json"


def already_done(key, event):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        done = json.loads(body)
        return done if done.get("status") == "ok" and done.get("kaggle_listed_bytes") == event["size"] else None
    except Exception:
        return None


def handler(event, context=None):
    started = time.time()
    version, date, chain = int(event["version"]), event["date"], event["chain"]
    path, listed = event["kaggle_path"], int(event["size"])
    name = path.split("/")[-1]
    key = f"source=kaggle/date={date}/chain={chain}/{name}.gz"
    mkey = manifest_key(event)

    done = already_done(mkey, event)
    if done:  # a restarted slice does not copy again what is already there
        return {**done, "skipped": True}

    md5 = hashlib.md5()
    raw_bytes = 0
    with tempfile.TemporaryDirectory() as tmp:
        gz_path = os.path.join(tmp, name + ".gz")
        with open_with_retries(kaggle_url(path, version)) as response:
            sent_length = response.headers.get("Content-Length")
            google_hash = response.headers.get("x-goog-hash", "")
            etag = (response.headers.get("ETag") or "").strip('"')
            # stream: read a chunk, hash it, gzip it to disk; /tmp only ever holds the small .gz
            with gzip.GzipFile(gz_path, "wb", compresslevel=5, mtime=0) as out:
                while True:
                    chunk = response.read(CHUNK)
                    if not chunk:
                        break
                    raw_bytes += len(chunk)
                    md5.update(chunk)
                    out.write(chunk)

        if raw_bytes != listed:
            raise RuntimeError(f"{path} v{version}: downloaded {raw_bytes} bytes, Kaggle lists {listed}")
        if sent_length is not None and int(sent_length) != raw_bytes:
            raise RuntimeError(f"{path} v{version}: Content-Length {sent_length}, received {raw_bytes}")
        google_md5 = None
        for part in google_hash.replace(" ", "").split(","):
            if part.startswith("md5="):
                google_md5 = b64decode(part[4:]).hex()
        if google_md5 and google_md5 != md5.hexdigest():
            raise RuntimeError(f"{path} v{version}: md5 {md5.hexdigest()} differs from Google's {google_md5}")

        # for ordinary Google Storage objects the ETag is the md5; it is recorded, not enforced
        md5_matches_etag = (md5.hexdigest() == etag) if len(etag) == 32 else None

        gz_bytes = os.path.getsize(gz_path)
        s3.upload_file(
            gz_path, BUCKET, key,
            ExtraArgs={"Metadata": {"kaggle-version": str(version), "raw-bytes": str(raw_bytes), "raw-md5": md5.hexdigest()}},
        )
        stored = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        if stored != gz_bytes:
            raise RuntimeError(f"{key}: S3 holds {stored} bytes, uploaded {gz_bytes}")

    result = {
        "status": "ok",
        "version": version,
        "date": date,
        "chain": chain,
        "kind": event["kind"],
        "kaggle_path": path,
        "kaggle_listed_bytes": listed,
        "downloaded_bytes": raw_bytes,
        "md5": md5.hexdigest(),
        "md5_checked_against_google": bool(google_md5),
        "md5_matches_etag": md5_matches_etag,
        "s3_key": key,
        "s3_bytes": stored,
        "secs": round(time.time() - started, 1),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    # the manifest is written last: its presence means the data file is complete
    s3.put_object(Bucket=BUCKET, Key=mkey, Body=json.dumps(result).encode(), ContentType="application/json")
    return result


if __name__ == "__main__":
    import sys

    print(json.dumps(handler(json.loads(sys.argv[1])), indent=2))
