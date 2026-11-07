#!/usr/bin/env python3
"""Small operator CLI. Starts AWS jobs, installs Athena views and checks results."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import boto3


ROOT = Path(__file__).resolve().parents[1]


def stack_outputs():
    process = subprocess.run([os.environ.get("TERRAFORM", "terraform"), f"-chdir={ROOT / 'terraform'}", "output", "-json"], check=True, capture_output=True, text=True)
    return {name: details["value"] for name, details in json.loads(process.stdout).items()}


class Pipeline:
    def __init__(self, outputs, region):
        self.outputs = outputs
        self.day = outputs["archive_day"]
        self.database = outputs["database_name"]
        self.glue = boto3.client("glue", region_name=region)
        self.athena = boto3.client("athena", region_name=region)

    def job(self, kind, hours="all", fail_before_publish=False, reset_day=False):
        """Start a Glue run and wait; a wait timeout does not stop the AWS job."""
        arguments = {"--DAY": self.day, "--HOURS": hours}
        if kind == "etl":
            arguments["--FAIL_BEFORE_PUBLISH"] = str(fail_before_publish).lower()
            arguments["--RESET_DAY"] = str(reset_day).lower()
        name = self.outputs[f"{kind}_job_name"]
        run_id = self.glue.start_job_run(JobName=name, Arguments=arguments)["JobRunId"]
        print(f"Started {name} {run_id} ({hours})", flush=True)
        deadline = time.monotonic() + 3600
        previous = None
        while time.monotonic() < deadline:
            run = self.glue.get_job_run(JobName=name, RunId=run_id)["JobRun"]
            state = run["JobRunState"]
            if state != previous:
                print(f"{run_id}: {state}", flush=True)
                previous = state
            if state == "SUCCEEDED":
                return run_id
            if state in {"FAILED", "TIMEOUT", "STOPPED", "ERROR", "EXPIRED"}:
                raise RuntimeError(f"{name} {run_id}: {state}: {run.get('ErrorMessage', '')}")
            time.sleep(15)
        raise TimeoutError(f"Operator wait expired; inspect the still-existing Glue run {run_id}")

    def rows(self, sql):
        """Execute fresh Athena SQL and stream data rows across all result pages."""
        execution_id = self.athena.start_query_execution(
            QueryString=sql, QueryExecutionContext={"Database": self.database},
            WorkGroup=self.outputs["athena_workgroup"],
            ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": False}},
        )["QueryExecutionId"]
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            execution = self.athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
            state = execution["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in {"FAILED", "CANCELLED"}:
                raise RuntimeError(f"Athena {execution_id}: {execution['Status'].get('StateChangeReason', state)}")
            time.sleep(2)
        else:
            self.athena.stop_query_execution(QueryExecutionId=execution_id)
            raise TimeoutError(f"Athena query exceeded five minutes: {execution_id}")
        print(f"Athena {execution_id}: {execution.get('Statistics', {}).get('DataScannedInBytes', 0)} bytes scanned", flush=True)
        # Athena includes the column-name header only in the first result page.
        skip_header = True
        for page in self.athena.get_paginator("get_query_results").paginate(QueryExecutionId=execution_id):
            result = page["ResultSet"]
            columns = [column["Name"] for column in result["ResultSetMetadata"]["ColumnInfo"]]
            for row in result["Rows"]:
                if skip_header:
                    skip_header = False
                    continue
                yield dict(zip(columns, [cell.get("VarCharValue") for cell in row["Data"]]))

    def query(self, sql):
        return list(self.rows(sql))

    def views(self):
        for path in sorted((ROOT / "sql").glob("*.sql")):
            sql = path.read_text().replace("${database}", self.database).replace("${archive_day}", self.day)
            self.query(sql)
            print(f"Installed {path.name}", flush=True)

    def fingerprint(self, excluded_hour=None):
        """Compare logical events across runs, excluding generated batch metadata."""
        where = "" if excluded_hour is None else f"WHERE archive_hour <> {int(excluded_hour)}"
        sql = f"SELECT archive_hour, event_id, repository, event_created_at FROM completed_openings {where} ORDER BY archive_hour, event_id"
        digest = hashlib.sha256()
        for row in self.rows(sql):
            digest.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        return digest.hexdigest()

    def verify(self, expected_hours):
        overview = self.query("SELECT * FROM overview")[0]
        hourly = self.query("SELECT * FROM hourly_status ORDER BY archive_hour")
        if len(hourly) != 24 or int(overview["completed_hours"]) != expected_hours:
            raise AssertionError(f"Expected {expected_hours}/24 completed hours: {overview}")
        total = int(overview["total_openings"])
        counts = self.query("SELECT count(*) AS event_count FROM completed_openings")[0]
        repositories = self.query("SELECT coalesce(sum(opening_count), 0) AS opening_count FROM repository_totals")[0]
        if total != int(counts["event_count"]) or total != int(repositories["opening_count"]):
            raise AssertionError("Completion counts, visible events and repository totals disagree")
        if any(row["opening_count"] is not None for row in hourly if row["processing_status"] == "MISSING"):
            raise AssertionError("A missing hour was represented as a zero")
        duplicate_ids = self.query("SELECT archive_hour, event_id FROM completed_openings GROUP BY archive_hour, event_id HAVING count(*) > 1 LIMIT 1")
        if duplicate_ids:
            raise AssertionError(f"Duplicate visible event: {duplicate_ids}")
        concentration = self.query("SELECT coalesce(sum(opening_count), 0) AS opening_count FROM busiest_hour_repositories")[0]
        if expected_hours and int(concentration["opening_count"]) != int(overview["busiest_hour_openings"]):
            raise AssertionError("Busiest-hour concentration does not reconcile to its hourly count")
        print(json.dumps({"overview": overview, "hours": [{key: row[key] for key in ["hour_utc", "processing_status", "opening_count"]} for row in hourly]}, indent=2), flush=True)
        return overview


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["views", "ingest", "etl", "prepare-demo", "finish-demo", "verify", "repeat-check", "failure-check"])
    parser.add_argument("--region", help="Override the region recorded in Terraform outputs")
    parser.add_argument("--hours", default="all", help="all or comma-separated archive hours for ingest/etl/repeat-check")
    parser.add_argument("--demo-hour", type=int, default=23)
    parser.add_argument("--expected-hours", type=int, default=24)
    parser.add_argument("--outputs", type=Path, help="Private terraform output -json file; otherwise read initialized stack")
    args = parser.parse_args()
    if not 0 <= args.demo_hour <= 23:
        parser.error("--demo-hour must be 0 through 23")
    outputs = {name: details["value"] for name, details in json.loads(args.outputs.read_text()).items()} if args.outputs else stack_outputs()
    pipeline = Pipeline(outputs, args.region or outputs["aws_region"])
    if args.command == "views":
        pipeline.views()
    elif args.command in {"ingest", "etl"}:
        pipeline.job(args.command, args.hours)
    elif args.command == "prepare-demo":
        pipeline.job("ingest")
        pipeline.job("etl", ",".join(str(hour) for hour in range(24) if hour != args.demo_hour), reset_day=True)
        pipeline.verify(23)
    elif args.command == "finish-demo":
        pipeline.job("etl", str(args.demo_hour))
        pipeline.verify(24)
    elif args.command == "verify":
        pipeline.verify(args.expected_hours)
    elif args.command == "repeat-check":
        pipeline.verify(args.expected_hours)
        before = pipeline.fingerprint()
        pipeline.job("etl", args.hours)
        pipeline.verify(args.expected_hours)
        after = pipeline.fingerprint()
        if before != after:
            raise AssertionError(f"Repeat changed logical events: {before} -> {after}")
        print(f"Repeat logical equality PASS: {after}")
    elif args.command == "failure-check":
        # Start from a complete day, deliberately orphan one output, verify the
        # failed hour is missing, then repair it and compare every logical event.
        pipeline.verify(24)
        before = pipeline.fingerprint()
        unaffected_before = pipeline.fingerprint(excluded_hour=args.demo_hour)
        try:
            pipeline.job("etl", str(args.demo_hour), fail_before_publish=True)
        except RuntimeError as error:
            if "Deliberate recovery rehearsal" not in str(error):
                raise
            print(f"Observed expected failure: {error}")
        else:
            raise AssertionError("Failure rehearsal unexpectedly succeeded")
        pipeline.verify(23)
        failed_hour = pipeline.query(f"SELECT processing_status, opening_count FROM hourly_status WHERE archive_hour = {args.demo_hour}")[0]
        if failed_hour["processing_status"] != "MISSING" or failed_hour["opening_count"] is not None:
            raise AssertionError("The deliberately failed hour remained visible")
        if unaffected_before != pipeline.fingerprint(excluded_hour=args.demo_hour):
            raise AssertionError("Failed retry changed unselected hours")
        pipeline.job("etl", str(args.demo_hour))
        pipeline.verify(24)
        if pipeline.fingerprint() != before:
            raise AssertionError("Failure recovery changed logical events")
        print("Failed retry visibility and repaired logical equality PASS")


if __name__ == "__main__":
    main()
