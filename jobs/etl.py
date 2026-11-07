#!/usr/bin/env python3
"""AWS Glue Spark entry point: valid PR openings plus atomic completion summaries.

An hour is visible only when its completion row selects a successfully written
immutable Parquet batch. The single Glue job has max_concurrent_runs=1: do not run
copies of this job against the same bucket.
"""

import argparse
import datetime as dt
import json
import logging
import uuid


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


def read_completions(s3, bucket, day):
    from botocore.exceptions import ClientError

    try:
        body = s3.get_object(Bucket=bucket, Key=f"curated/completions/{day}.json")["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return []
        raise
    rows = [json.loads(line) for line in body.splitlines() if line.strip()]
    if any(row["archive_day"] != day or row["archive_hour"] not in range(24) for row in rows):
        raise ValueError("Invalid completion ledger; refusing to overwrite it")
    if len({row["archive_hour"] for row in rows}) != len(rows):
        raise ValueError("Duplicate hours in completion ledger")
    return rows


def write_completions(s3, bucket, day, rows):
    ordered_rows = sorted(rows, key=lambda row: row["archive_hour"])
    body = "".join(json.dumps(row, sort_keys=True) + "\n" for row in ordered_rows)
    s3.put_object(
        Bucket=bucket,
        Key=f"curated/completions/{day}.json",
        Body=body.encode(),
        ContentType="application/x-ndjson",
        ServerSideEncryption="AES256",
    )


def publish_batch(s3, bucket, day, hours, build_and_write, reset_day=False):
    """Invalidate selected hours, write the entire batch, then publish atomically.

    Any read, transform or output failure leaves those hours missing. Existing
    unselected hours remain queryable. Orphan Parquet is harmless because every
    report joins to the completion ledger.
    """
    previous = read_completions(s3, bucket, day)
    retained = [] if reset_day else [row for row in previous if row["archive_hour"] not in hours]
    write_completions(s3, bucket, day, retained)
    summaries = build_and_write()
    if sorted(row["archive_hour"] for row in summaries) != hours:
        raise ValueError("Output summaries do not exactly cover the requested hours")
    write_completions(s3, bucket, day, retained + summaries)
    return summaries


def raw_schema():
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("created_at", StringType()),
        StructField("repo", StructType([StructField("name", StringType())])),
        StructField("payload", StructType([StructField("action", StringType())])),
        StructField("_corrupt_record", StringType()),
    ])


def extract_openings(raw, day, batch_id):
    """Keep event IDs once per archive hour; preserve source event timestamps.

    archive_hour is the file's UTC hour, not a claim about API delivery latency.
    Identical duplicates collapse. Conflicting records for one event ID make the
    batch fail rather than choosing a nondeterministic repository.
    """
    from pyspark.sql import functions as F

    candidates = raw.filter((F.col("type") == "PullRequestEvent") & (F.col("payload.action") == "opened"))
    projected = candidates.select(
        F.col("id").alias("event_id"), F.col("repo.name").alias("repository"),
        F.col("created_at").alias("event_created_at"), "archive_hour", "_corrupt_record",
    )
    valid = projected.filter(
        F.col("_corrupt_record").isNull()
        & F.col("event_id").rlike(r"^[0-9]+$")
        & F.col("repository").rlike(r"^[^/\s]+/[^/\s]+$")
        & F.to_timestamp("event_created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").isNotNull()
        & F.col("archive_hour").between(0, 23)
    ).drop("_corrupt_record")
    unique = valid.distinct()
    conflicts = unique.groupBy("archive_hour", "event_id").count().filter("count > 1").limit(1).count()
    if conflicts:
        raise ValueError("Conflicting PR-opening records share an event ID within an archive hour")
    events = unique.withColumn("archive_day", F.lit(day)).withColumn("batch_id", F.lit(batch_id))
    return candidates, valid, events


def process_hours(spark, s3, bucket, day, hours, fail_before_publish=False, input_paths=None, output_path=None):
    """Write one immutable batch and return a completion row for every hour.

    Zero-opening hours still receive a row. This function does not publish the
    rows: publish_batch owns that visibility change after this function returns.
    input_paths and output_path allow the same transformation to run locally.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    batch_id = uuid.uuid4().hex
    paths = input_paths or [f"s3://{bucket}/raw/day={day}/{day}-{hour}.json.gz" for hour in hours]
    if input_paths is None:
        for hour in hours:
            metadata = s3.head_object(Bucket=bucket, Key=f"raw/day={day}/{day}-{hour}.json.gz").get("Metadata", {})
            if metadata.get("validated-gzip") != "true":
                raise ValueError(f"Raw hour {hour} has not passed ingestion validation")
    raw = spark.read.schema(raw_schema()).option("mode", "PERMISSIVE").json(paths).withColumn(
        "archive_hour", F.regexp_extract(F.input_file_name(), r"-(\d{1,2})\.json\.gz$", 1).cast("int")
    ).persist(StorageLevel.MEMORY_AND_DISK)
    events = None
    try:
        raw_counts = {row.archive_hour: row["count"] for row in raw.groupBy("archive_hour").count().collect()}
        if any(hour not in hours for hour in raw_counts):
            raise ValueError("Input filename does not identify a requested archive hour")
        candidates, valid, events = extract_openings(raw, day, batch_id)
        events = events.persist(StorageLevel.MEMORY_AND_DISK)
        candidate_counts = {row.archive_hour: row["count"] for row in candidates.groupBy("archive_hour").count().collect()}
        valid_counts = {row.archive_hour: row["count"] for row in valid.groupBy("archive_hour").count().collect()}
        opening_counts = {row.archive_hour: row["count"] for row in events.groupBy("archive_hour").count().collect()}
        destination = output_path or f"s3://{bucket}/curated/events/{batch_id}/"
        events.coalesce(4).write.mode("errorifexists").parquet(destination, compression="snappy")
        # A real deliberate failure point for the recovery rehearsal. No completion
        # is ever published for these otherwise fully written orphan files.
        if fail_before_publish:
            raise RuntimeError("Deliberate recovery rehearsal: failed after Parquet write, before completion publication")
        completed_at = dt.datetime.now(dt.timezone.utc).isoformat()
        return [{
            "archive_day": day, "archive_hour": hour, "batch_id": batch_id,
            "raw_events": raw_counts.get(hour, 0), "opening_candidates": candidate_counts.get(hour, 0),
            "rejected_openings": candidate_counts.get(hour, 0) - valid_counts.get(hour, 0),
            "duplicate_openings": valid_counts.get(hour, 0) - opening_counts.get(hour, 0),
            "opening_count": opening_counts.get(hour, 0), "completed_at": completed_at,
        } for hour in hours]
    finally:
        raw.unpersist()
        if events is not None:
            events.unpersist()


def main():
    import boto3
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--BUCKET", required=True)
    parser.add_argument("--DAY", required=True)
    parser.add_argument("--HOURS", default="all")
    parser.add_argument("--FAIL_BEFORE_PUBLISH", choices=["true", "false"], default="false")
    parser.add_argument("--RESET_DAY", choices=["true", "false"], default="false", help="Invalidate the entire day before processing the selected hours")
    args, _ = parser.parse_known_args()
    day = dt.date.fromisoformat(args.DAY).isoformat()
    if day != args.DAY:
        raise ValueError("DAY must use YYYY-MM-DD")
    hours = parse_hours(args.HOURS)
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("CIS551 GitHub PR openings").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    try:
        s3 = boto3.client("s3")

        def build_and_write():
            return process_hours(
                spark, s3, args.BUCKET, day, hours,
                fail_before_publish=args.FAIL_BEFORE_PUBLISH == "true",
            )

        summaries = publish_batch(
            s3, args.BUCKET, day, hours, build_and_write,
            reset_day=args.RESET_DAY == "true",
        )
        logging.info("Completed batch: %s", json.dumps(summaries, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
