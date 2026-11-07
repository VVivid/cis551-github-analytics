"""Offline operator tests: stub AWS responses and advance waits without sleeping."""

import io
import unittest
from unittest.mock import call, patch

import boto3
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


if __name__ == "__main__":
    unittest.main()
