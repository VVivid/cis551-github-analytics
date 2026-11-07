"""Offline operator tests: stub AWS responses and advance waits without sleeping."""

import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, call, patch

import boto3
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ClientError, ReadTimeoutError
from botocore.stub import Stubber

from scripts.run import Pipeline


class GlueStartTests(unittest.TestCase):
    def setUp(self):
        self.glue = boto3.client("glue", region_name="us-east-2", aws_access_key_id="test", aws_secret_access_key="test")
        self.stubber = Stubber(self.glue)
        self.stubber.activate()
        self.addCleanup(self.stubber.deactivate)
        self.addCleanup(self.stubber.assert_no_pending_responses)
        self.pipeline = object.__new__(Pipeline)
        self.pipeline.glue = self.glue
        self.pipeline.glue_start_client = self.glue
        self.pipeline.day = "2024-10-15"
        self.pipeline.outputs = {"etl_job_name": "demo-etl"}
        self.start_parameters = {
            "JobName": "demo-etl",
            "Arguments": {"--DAY": self.pipeline.day, "--HOURS": "23", "--FAIL_BEFORE_PUBLISH": "false", "--RESET_DAY": "false"},
        }
        self.elapsed = 0
        clock_patch = patch("scripts.run.time.monotonic", side_effect=lambda: self.elapsed)
        clock_patch.start()
        self.addCleanup(clock_patch.stop)
        sleep_patch = patch("scripts.run.time.sleep", side_effect=self.advance)
        self.sleep = sleep_patch.start()
        self.addCleanup(sleep_patch.stop)
        output_patch = patch("sys.stdout", new_callable=io.StringIO)
        self.output = output_patch.start()
        self.addCleanup(output_patch.stop)

    def advance(self, seconds):
        self.elapsed += seconds

    def reject_start(self):
        self.stubber.add_client_error(
            "start_job_run", service_error_code="ConcurrentRunsExceededException",
            service_message="Maximum concurrent runs reached", expected_params=self.start_parameters,
        )

    def accept_start(self):
        self.stubber.add_response("start_job_run", {"JobRunId": "jr_accepted"}, self.start_parameters)

    def run_state(self, state, message=""):
        self.stubber.add_response(
            "get_job_run", {"JobRun": {"JobRunState": state, "ErrorMessage": message}},
            {"JobName": "demo-etl", "RunId": "jr_accepted"},
        )

    def test_rejected_starts_eventually_accept_and_poll_one_run(self):
        self.reject_start()
        self.reject_start()
        self.accept_start()
        self.run_state("RUNNING")
        self.run_state("SUCCEEDED")

        self.assertEqual(self.pipeline.job("etl", "23"), "jr_accepted")

        self.assertEqual(self.sleep.call_args_list, [call(10), call(10), call(15)])
        self.assertEqual(self.output.getvalue().count("Glue rejected the start"), 2)
        self.assertEqual(self.output.getvalue().count("Started demo-etl jr_accepted"), 1)

    def test_rejected_starts_stop_at_two_minute_deadline(self):
        for _ in range(12):
            self.reject_start()

        with self.assertRaises(self.glue.exceptions.ConcurrentRunsExceededException):
            self.pipeline.job("etl", "23")

        self.assertEqual(self.elapsed, 120)
        self.assertEqual(self.sleep.call_args_list, [call(10)] * 12)
        self.assertNotIn("Started", self.output.getvalue())

    def test_other_errors_are_not_retried(self):
        errors = [
            ClientError({"Error": {"Code": "AccessDeniedException", "Message": "Denied"}}, "StartJobRun"),
            ReadTimeoutError(endpoint_url="https://glue.invalid"),
        ]
        for error in errors:
            with self.subTest(error=type(error).__name__), patch.object(self.glue, "start_job_run", side_effect=error) as start:
                with self.assertRaises(type(error)) as raised:
                    self.pipeline.job("etl", "23")
                self.assertIs(raised.exception, error)
                start.assert_called_once_with(**self.start_parameters)
        self.sleep.assert_not_called()

    def test_accepted_run_failure_is_not_restarted(self):
        self.accept_start()
        self.run_state("FAILED", "ETL failed")

        with self.assertRaisesRegex(RuntimeError, "demo-etl jr_accepted: FAILED: ETL failed"):
            self.pipeline.job("etl", "23")

        self.sleep.assert_not_called()
        self.assertEqual(self.output.getvalue().count("Started demo-etl jr_accepted"), 1)


class GlueSdkRetryTests(unittest.TestCase):
    def setUp(self):
        environment_patch = patch.dict(os.environ, {
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_MAX_ATTEMPTS": "5",
            "AWS_RETRY_MODE": "legacy",
        })
        environment_patch.start()
        self.addCleanup(environment_patch.stop)
        os.environ.pop("AWS_PROFILE", None)
        os.environ.pop("AWS_DEFAULT_PROFILE", None)
        sleep_patch = patch("scripts.run.time.sleep")
        self.sleep = sleep_patch.start()
        self.addCleanup(sleep_patch.stop)
        output_patch = patch("sys.stdout", new_callable=io.StringIO)
        output_patch.start()
        self.addCleanup(output_patch.stop)

    def pipeline(self):
        session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test")
        with patch("scripts.run.boto3.client", side_effect=session.client):
            return Pipeline({"archive_day": "2024-10-15", "database_name": "demo", "etl_job_name": "demo-etl"}, "us-east-2")

    def response(self, body):
        raw = Mock()
        raw.stream.return_value = [json.dumps(body).encode()]
        return AWSResponse("https://glue.us-east-2.amazonaws.com", 200, {"content-type": "application/x-amz-json-1.1"}, raw)

    def assert_ambiguous_start_is_sent_once(self):
        pipeline = self.pipeline()
        timeout = ReadTimeoutError(endpoint_url=pipeline.glue_start_client.meta.endpoint_url)
        # A second transport send would accept another run if retries were enabled.
        starts = [timeout, self.response({"JobRunId": "jr_accepted"})]
        polls = [self.response({"JobRun": {"JobRunState": "SUCCEEDED"}})]
        with patch.object(pipeline.glue_start_client._endpoint.http_session, "send", side_effect=starts) as start_send, \
                patch.object(pipeline.glue._endpoint.http_session, "send", side_effect=polls) as poll_send:
            with self.assertRaises(ReadTimeoutError) as raised:
                pipeline.job("etl", "23")

            self.assertIs(raised.exception, timeout)
            start_send.assert_called_once()
            poll_send.assert_not_called()
        self.sleep.assert_not_called()

    def test_read_timeout_is_not_retried_under_environment_defaults(self):
        for mode in ["legacy", "standard", "adaptive"]:
            with self.subTest(mode=mode), patch.dict(os.environ, {"AWS_RETRY_MODE": mode}):
                self.assert_ambiguous_start_is_sent_once()

    def test_read_timeout_is_not_retried_under_profile_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config"
            config.write_text("[default]\nretry_mode = standard\nmax_attempts = 7\n")
            with patch.dict(os.environ, {"AWS_CONFIG_FILE": str(config)}):
                os.environ.pop("AWS_MAX_ATTEMPTS")
                os.environ.pop("AWS_RETRY_MODE")
                self.assert_ambiguous_start_is_sent_once()

    def test_polling_keeps_sdk_retries_without_restarting_accepted_run(self):
        pipeline = self.pipeline()
        starts = [self.response({"JobRunId": "jr_accepted"})]
        polls = [
            ReadTimeoutError(endpoint_url=pipeline.glue.meta.endpoint_url),
            self.response({"JobRun": {"JobRunState": "SUCCEEDED"}}),
        ]
        with patch.object(pipeline.glue_start_client._endpoint.http_session, "send", side_effect=starts) as start_send, \
                patch.object(pipeline.glue._endpoint.http_session, "send", side_effect=polls) as poll_send:
            self.assertEqual(pipeline.job("etl", "23"), "jr_accepted")

            start_send.assert_called_once()
            self.assertEqual(poll_send.call_count, 2)
            for request in poll_send.call_args_list:
                self.assertEqual(json.loads(request.args[0].body), {"JobName": "demo-etl", "RunId": "jr_accepted"})


if __name__ == "__main__":
    unittest.main()
