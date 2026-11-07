"""Offline regression: real Spark + Parquet + the same SQL used by Athena."""

import gzip
import io
import json
from collections import Counter
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError
import duckdb
from pyspark.sql import SparkSession

from jobs import etl, ingest


DAY = "2024-10-15"
ROOT = Path(__file__).resolve().parents[1]


class MemoryS3:
    """Only the S3 calls exercised here; writes replace one object atomically."""

    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key]), "Metadata": {"validated-gzip": "true"}}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs):
        self.objects[Key] = Path(Filename).read_bytes()


def completion(hour, batch="old", count=1):
    return {"archive_day": DAY, "archive_hour": hour, "batch_id": batch, "raw_events": count, "opening_candidates": count, "rejected_openings": 0, "duplicate_openings": 0, "opening_count": count, "completed_at": "2026-11-07T00:00:00+00:00"}


class IngestionAndCommitTests(unittest.TestCase):
    def test_hour_selection_and_dates(self):
        for module in [etl, ingest]:
            self.assertEqual(module.parse_hours("all"), list(range(24)))
            self.assertEqual(module.parse_hours("23,2,2,0"), [0, 2, 23])
            for value in ["", "24", "-1", "a", "1,"]:
                with self.assertRaises(ValueError):
                    module.parse_hours(value)
        self.assertEqual(ingest.validate_day(DAY), DAY)
        with self.assertRaises(ValueError):
            ingest.validate_day("2024-02-30")

    def test_ingestion_validates_gzip_before_upload_and_reuses_valid_objects(self):
        s3 = MemoryS3()
        compressed = gzip.compress(b'{"id":"1"}\n')
        with patch("urllib.request.urlopen", return_value=io.BytesIO(compressed)):
            result = ingest.fetch_hour(s3, "test", DAY, 0)
        self.assertFalse(result["reused"])
        self.assertEqual(result["raw_lines"], 1)
        with patch("urllib.request.urlopen", side_effect=AssertionError("must not download again")):
            self.assertTrue(ingest.fetch_hour(s3, "test", DAY, 0)["reused"])
        with patch("urllib.request.urlopen", side_effect=lambda *args, **kwargs: io.BytesIO(compressed[:-4])), patch("time.sleep"):
            with self.assertRaises(EOFError):
                ingest.fetch_hour(s3, "test", DAY, 1)
        self.assertNotIn(f"raw/day={DAY}/{DAY}-1.json.gz", s3.objects)

    def test_failed_retry_invalidates_selected_hours_and_preserves_other_hours(self):
        s3 = MemoryS3()
        etl.write_completions(s3, "test", DAY, [completion(0), completion(1)])

        def fail():
            self.assertEqual([row["archive_hour"] for row in etl.read_completions(s3, "test", DAY)], [1])
            raise RuntimeError("output failed")

        with self.assertRaisesRegex(RuntimeError, "output failed"):
            etl.publish_batch(s3, "test", DAY, [0], fail)
        self.assertEqual(etl.read_completions(s3, "test", DAY), [completion(1)])
        etl.publish_batch(s3, "test", DAY, [0], lambda: [completion(0, "new", 0)])
        self.assertEqual([row["opening_count"] for row in etl.read_completions(s3, "test", DAY)], [0, 1])


class SparkAndSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("CIS551 regression").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.conf.set("spark.sql.session.timeZone", "UTC")
        cls.spark.conf.set("spark.sql.ansi.enabled", "false")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / "raw"
        self.raw.mkdir()
        self.outputs = self.root / "outputs"
        self.outputs.mkdir()
        self.s3 = MemoryS3()
        self.expected_events = []
        self.paths = {}
        for hour in range(24):
            records = [{"id": str(hour), "type": "PushEvent"}, {"id": str(100 + hour), "type": "PullRequestEvent", "payload": {"action": "closed"}}]
            count = 0 if hour == 0 else 13 if hour == 17 else 1 + hour % 4
            for position in range(count):
                event = {"id": str(10000 + hour * 100 + position), "type": "PullRequestEvent", "repo": {"name": f"owner/repo-{(hour + position) % 12:02d}"}, "created_at": f"{DAY}T{hour:02d}:00:00Z", "payload": {"action": "opened"}}
                records.append(event)
                self.expected_events.append((hour, event["id"], event["repo"]["name"], event["created_at"]))
            if hour == 2:
                invalid = dict(records[-1], id="999999", created_at="invalid-time")
                records.append(invalid)
            if hour == 5:
                records.append(records[-1].copy())
            path = self.raw / f"{DAY}-{hour}.json.gz"
            with gzip.open(path, "wt") as output:
                for record in records:
                    output.write(json.dumps(record) + "\n")
                if hour == 4:
                    output.write("{not-valid-json\n")
            self.paths[hour] = str(path)

    def process(self, hours, name, fail=False, reset_day=False):
        return etl.publish_batch(self.s3, "test", DAY, hours, lambda: etl.process_hours(
            self.spark, self.s3, "test", DAY, hours, fail_before_publish=fail,
            input_paths=[self.paths[hour] for hour in hours], output_path=str(self.outputs / name),
        ), reset_day=reset_day)

    def database(self):
        connection = duckdb.connect()
        self.addCleanup(connection.close)
        connection.execute("CREATE SCHEMA analytics")
        connection.execute("CREATE TABLE analytics.opening_events(event_id VARCHAR, repository VARCHAR, event_created_at VARCHAR, archive_day VARCHAR, archive_hour INTEGER, batch_id VARCHAR)")
        parquet = sorted(str(path) for path in self.outputs.glob("*/*.parquet"))
        if parquet:
            connection.execute("INSERT INTO analytics.opening_events BY NAME SELECT * FROM read_parquet(?, union_by_name=true)", [parquet])
        connection.execute("CREATE TABLE analytics.hour_completions(archive_day VARCHAR, archive_hour INTEGER, batch_id VARCHAR, raw_events BIGINT, opening_candidates BIGINT, rejected_openings BIGINT, duplicate_openings BIGINT, opening_count BIGINT, completed_at VARCHAR)")
        columns = ["archive_day", "archive_hour", "batch_id", "raw_events", "opening_candidates", "rejected_openings", "duplicate_openings", "opening_count", "completed_at"]
        rows = etl.read_completions(self.s3, "test", DAY)
        if rows:
            connection.executemany("INSERT INTO analytics.hour_completions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [[row[column] for column in columns] for row in rows])
        for path in sorted((ROOT / "sql").glob("*.sql")):
            connection.execute(path.read_text().replace("${database}", "analytics").replace("${archive_day}", DAY))
        return connection

    @staticmethod
    def logical_rows(database):
        return database.execute("SELECT archive_hour, event_id, repository, event_created_at FROM analytics.completed_openings ORDER BY archive_hour, event_id").fetchall()

    def test_full_day_split_repeat_and_partial_failure_have_consistent_results(self):
        initial = self.database()
        self.assertEqual(initial.execute("SELECT report_status, completed_hours, missing_hours, total_openings, busiest_hour_utc, top_10_share_percent FROM analytics.overview").fetchone(), ("PROVISIONAL", 0, 24, 0, None, None))
        # One Spark startup processes the whole day, including a valid zero hour.
        summaries = self.process(list(range(24)), "full-day")
        self.assertEqual(len(summaries), 24)
        self.assertEqual(summaries[2]["rejected_openings"], 1)
        self.assertEqual(summaries[5]["duplicate_openings"], 1)
        database = self.database()
        expected = sorted(self.expected_events)
        self.assertEqual(self.logical_rows(database), expected)
        overview = database.execute("SELECT report_status, completed_hours, missing_hours, total_openings, busiest_hour_utc, busiest_hour_openings, top_10_share_percent FROM analytics.overview").fetchone()
        self.assertEqual(overview[:6], ("FULL DAY", 24, 0, len(expected), "17:00", 13))
        counts = Counter(row[2] for row in expected)
        ranked = sorted(counts.items(), key=lambda row: (-row[1], row[0]))
        expected_top_ten_share = 100 * sum(count for _, count in ranked[:10]) / len(expected)
        self.assertAlmostEqual(float(overview[6]), expected_top_ten_share)
        self.assertEqual(database.execute("SELECT repository, opening_count FROM analytics.repository_totals ORDER BY repository_rank").fetchall(), ranked)
        self.assertEqual(database.execute("SELECT processing_status, opening_count FROM analytics.hourly_status WHERE archive_hour=0").fetchone(), ("COMPLETED", 0))
        self.assertEqual(database.execute("SELECT sum(opening_count) FROM analytics.repository_totals").fetchone()[0], len(expected))
        busiest_totals = database.execute("SELECT sum(opening_count), sum(share_percent) FROM analytics.busiest_hour_repositories").fetchone()
        self.assertEqual(busiest_totals[0], 13)
        self.assertAlmostEqual(busiest_totals[1], 100.0)

        # The same 24 logical hours produced as two batches match the full-day run.
        self.process(list(range(23)), "first-23", reset_day=True)
        partial = self.database()
        self.assertEqual(self.logical_rows(partial), [row for row in expected if row[0] != 23])
        self.assertEqual(partial.execute("SELECT processing_status, opening_count FROM analytics.hourly_status WHERE archive_hour=23").fetchone(), ("MISSING", None))
        self.process([23], "last-hour")
        self.assertEqual(self.logical_rows(self.database()), expected)

        # A failed retry leaves orphan files, but only the selected hour disappears.
        with self.assertRaisesRegex(RuntimeError, "Deliberate recovery rehearsal"):
            self.process([23], "failed-output", fail=True)
        failed = self.database()
        self.assertEqual(failed.execute("SELECT report_status, completed_hours, missing_hours FROM analytics.overview").fetchone(), ("PROVISIONAL", 23, 1))
        self.assertEqual(failed.execute("SELECT processing_status, opening_count FROM analytics.hourly_status WHERE archive_hour=23").fetchone(), ("MISSING", None))
        self.assertEqual(self.logical_rows(failed), [row for row in expected if row[0] != 23])

        self.process([23], "repaired")
        self.assertEqual(self.logical_rows(self.database()), expected)
        # Reprocessing the zero hour cannot turn it into a missing hour.
        self.process([0], "repeat-zero")
        repaired = self.database()
        self.assertEqual(self.logical_rows(repaired), expected)
        self.assertEqual(repaired.execute("SELECT processing_status, opening_count FROM analytics.hourly_status WHERE archive_hour=0").fetchone(), ("COMPLETED", 0))

    def test_conflicting_same_id_fails_the_batch(self):
        path = Path(self.paths[1])
        with gzip.open(path, "rt") as input_file:
            records = [json.loads(line) for line in input_file]
        records.append(dict(records[-1], repo={"name": "different/repository"}))
        with gzip.open(path, "wt") as output:
            output.write("".join(json.dumps(record) + "\n" for record in records))
        with self.assertRaisesRegex(ValueError, "Conflicting PR-opening records"):
            self.process([1], "conflict")
        self.assertEqual(etl.read_completions(self.s3, "test", DAY), [])


if __name__ == "__main__":
    unittest.main()
