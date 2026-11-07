#!/usr/bin/env python3
"""AWS Glue Python-shell entry point: fetch selected GH Archive hours into S3."""

import argparse
import datetime as dt
import gzip
import hashlib
import json
import logging
import tempfile
import time
import urllib.request
from pathlib import Path


def parse_hours(value):
    if value == "all":
        return list(range(24))
    try:
        hours = sorted({int(part) for part in value.split(",")})
    except ValueError as error:
        raise ValueError("HOURS must be all or comma-separated integers from 0 to 23") from error
    if not hours or any(hour < 0 or hour > 23 for hour in hours):
        raise ValueError("HOURS must be all or comma-separated integers from 0 to 23")
    return hours


def validate_day(value):
    if dt.date.fromisoformat(value).isoformat() != value:
        raise ValueError("DAY must use YYYY-MM-DD")
    return value


def fetch_hour(s3, bucket, day, hour, overwrite=False):
    from botocore.exceptions import ClientError

    filename = f"{day}-{hour}.json.gz"
    key = f"raw/day={day}/{filename}"
    if not overwrite:
        try:
            existing = s3.head_object(Bucket=bucket, Key=key)
            if existing.get("Metadata", {}).get("validated-gzip") == "true":
                logging.info("Already loaded: s3://%s/%s", bucket, key)
                return {"hour": hour, "key": key, "bytes": existing["ContentLength"], "reused": True}
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}:
                raise

    # Validate the complete gzip stream (including its CRC) before publishing raw data.
    # Only one archive is held on local disk at a time.
    url = f"https://data.gharchive.org/{filename}"
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / filename
        for attempt in range(3):
            try:
                digest = hashlib.sha256()
                request = urllib.request.Request(url, headers={"User-Agent": "CIS551-GHArchive-Analytics/1.0"})
                with urllib.request.urlopen(request, timeout=120) as response, path.open("wb") as output:
                    while chunk := response.read(1024 * 1024):
                        digest.update(chunk)
                        output.write(chunk)
                if path.stat().st_size == 0:
                    raise ValueError(f"Empty archive: {url}")
                lines = 0
                with gzip.open(path, "rb") as archive:
                    for line in archive:
                        if line.strip():
                            lines += 1
                break
            except Exception:
                if attempt == 2:
                    raise
                logging.exception("Download/validation failed; retrying hour %s", hour)
                time.sleep(2 ** attempt)

        s3.upload_file(str(path), bucket, key, ExtraArgs={
            "ContentType": "application/gzip",
            "ServerSideEncryption": "AES256",
            "Metadata": {"source-url": url, "sha256": digest.hexdigest(), "validated-gzip": "true", "nonempty-lines": str(lines)},
        })
        result = {"hour": hour, "key": key, "bytes": path.stat().st_size, "raw_lines": lines, "reused": False}
        logging.info("Loaded %s", json.dumps(result, sort_keys=True))
        return result


def main():
    import boto3

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--BUCKET", required=True)
    parser.add_argument("--DAY", required=True)
    parser.add_argument("--HOURS", default="all")
    parser.add_argument("--OVERWRITE", choices=["true", "false"], default="false")
    args, _ = parser.parse_known_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    day = validate_day(args.DAY)
    results = [fetch_hour(boto3.client("s3"), args.BUCKET, day, hour, args.OVERWRITE == "true") for hour in parse_hours(args.HOURS)]
    logging.info("Ingestion complete: %s", json.dumps({"day": day, "hours": results}, sort_keys=True))


if __name__ == "__main__":
    main()
