"""Verify first deployment, safe updates and bucket discovery without AWS calls."""

from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import io
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, call, patch
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops"))
import deploy
import upload_delivery

VARIABLES = {
    "TARGET_ENVIRONMENT": "dev",
    "SOURCE_BRANCH": "dev",
    "PROJECT": "meesho-dev",
    "DATABASE_PREFIX": "meesho_dev",
    "ARTIFACT_BUCKET": "artifacts",
    "KEY_ARN": "key",
    "DEPLOYMENT_ROLE_ARN": "role",
    "CODEBUILD_RESOLVED_SOURCE_VERSION": "abc123",
    "CODEBUILD_BUILD_NUMBER": "7",
}
FOUNDATION = {"Outputs": [{"OutputKey": "DataBucket", "OutputValue": "data-bucket"}]}


class DeploymentTests(unittest.TestCase):
    def boundaries(self, previous=None):
        stack = ExitStack()
        self.addCleanup(stack.close)
        mocked = {
            name: stack.enter_context(patch.object(deploy, name))
            for name in [
                "describe_stack",
                "deploy_stack",
                "drain",
                "publish",
                "verify_workflow",
                "schedule",
                "invoke",
            ]
        }
        stack.enter_context(patch.object(deploy.boto3, "client"))
        stack.enter_context(patch.object(deploy.boto3, "resource"))
        mocked["describe_stack"].side_effect = [previous, FOUNDATION]
        return mocked

    def test_first_push_creates_both_stacks_without_existing_runtime(self):
        mocked = self.boundaries()
        deploy.deploy(VARIABLES)
        mocked["drain"].assert_not_called()
        self.assertEqual(
            [c.args[1] for c in mocked["deploy_stack"].call_args_list],
            ["foundation", "platform"],
        )
        platform = mocked["deploy_stack"].call_args_list[1].args[3]
        self.assertEqual(platform["DataBucket"], "data-bucket")
        self.assertEqual(platform["ScheduleState"], "DISABLED")
        mocked["publish"].assert_called_once_with("artifacts", "dev-abc123-7")
        mocked["schedule"].assert_called_once_with("meesho-dev", "ENABLED")
        mocked["invoke"].assert_called_once_with("meesho-dev", {"action": "resume"})

    def test_failed_update_never_resumes_dispatch(self):
        mocked = self.boundaries(previous={"StackStatus": "CREATE_COMPLETE"})
        mocked["deploy_stack"].side_effect = [None, RuntimeError("stack failed")]
        with self.assertRaisesRegex(RuntimeError, "stack failed"):
            deploy.deploy(VARIABLES)
        mocked["drain"].assert_called_once()
        mocked["schedule"].assert_not_called()
        mocked["invoke"].assert_not_called()
        mocked["verify_workflow"].assert_not_called()

    def test_wrong_branch_is_rejected_before_aws_access(self):
        with patch.object(deploy.boto3, "client") as client:
            with self.assertRaises(ValueError):
                deploy.deploy({**VARIABLES, "SOURCE_BRANCH": "main"})
        client.assert_not_called()

    def test_incomplete_platform_stops_before_maintenance_or_deployment(self):
        for status in [
            "ROLLBACK_FAILED",
            "ROLLBACK_COMPLETE",
            "CREATE_FAILED",
            "UPDATE_IN_PROGRESS",
            "UPDATE_ROLLBACK_FAILED",
            "DELETE_FAILED",
        ]:
            with self.subTest(status=status):
                mocked = self.boundaries(previous={"StackStatus": status})
                with self.assertRaisesRegex(RuntimeError, status):
                    deploy.deploy(VARIABLES)
                for name in ["drain", "deploy_stack", "publish", "schedule", "invoke"]:
                    mocked[name].assert_not_called()

    def test_stable_platform_can_be_updated_and_resumed(self):
        for status in [
            "CREATE_COMPLETE",
            "UPDATE_COMPLETE",
            "UPDATE_ROLLBACK_COMPLETE",
        ]:
            with self.subTest(status=status):
                mocked = self.boundaries(previous={"StackStatus": status})
                deploy.deploy(VARIABLES)
                mocked["drain"].assert_called_once()
                mocked["invoke"].assert_called_once_with(
                    "meesho-dev", {"action": "resume"}
                )

    def test_failure_log_includes_current_attempt_across_event_pages(self):
        start = datetime(2026, 10, 16, 20, 50, tzinfo=timezone.utc)

        def event(seconds, resource, status, reason):
            return {
                "Timestamp": start + timedelta(seconds=seconds),
                "LogicalResourceId": resource,
                "ResourceStatus": status,
                "ResourceStatusReason": reason,
            }

        client = MagicMock()
        client.get_paginator.return_value.paginate.return_value = [
            {
                "StackEvents": [
                    event(30, "BronzeDatabase", "DELETE_FAILED", "AccessDenied")
                ]
            },
            {
                "StackEvents": [
                    event(20, "Crawler", "CREATE_FAILED", "Invalid table level"),
                    event(10, "RawDatabase", "CREATE_COMPLETE", "Created"),
                    event(-60, "OldAttempt", "CREATE_FAILED", "Old error"),
                ]
            },
        ]
        output = io.StringIO()
        with patch.object(deploy.boto3, "client", return_value=client), patch(
            "sys.stdout", output
        ):
            deploy.report_stack_failure("meesho-dev-platform", start)
        log = output.getvalue()
        self.assertIn("Crawler | CREATE_FAILED | Invalid table level", log)
        self.assertIn("BronzeDatabase | DELETE_FAILED | AccessDenied", log)
        self.assertLess(log.index("Crawler"), log.index("BronzeDatabase"))
        self.assertNotIn("OldAttempt", log)
        self.assertNotIn("RawDatabase", log)

    def test_event_read_failure_preserves_original_cli_exception(self):
        original = deploy.subprocess.CalledProcessError(
            255, ["aws", "cloudformation", "deploy"]
        )
        denied = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}},
            "DescribeStackEvents",
        )
        client = MagicMock()
        client.get_paginator.return_value.paginate.side_effect = denied
        with patch.object(deploy.subprocess, "run", side_effect=original), patch.object(
            deploy.boto3, "client", return_value=client
        ), patch("sys.stdout", new_callable=io.StringIO) as output:
            with self.assertRaises(deploy.subprocess.CalledProcessError) as caught:
                deploy.deploy_stack("meesho-dev", "platform", "role", {})
        self.assertIs(caught.exception, original)
        self.assertIn("Could not read stack events", output.getvalue())

    def test_successful_stack_deployment_does_not_fetch_failure_events(self):
        with patch.object(deploy.subprocess, "run"), patch.object(
            deploy.boto3, "client"
        ) as client:
            deploy.deploy_stack("meesho-dev", "platform", "role", {})
        client.assert_not_called()

    def test_readiness_requires_crawler_and_all_three_jobs(self):
        client = MagicMock()
        nodes = [
            {"Type": "CRAWLER", "Name": "meesho-dev-raw-discovery"},
            *[
                {"Type": "JOB", "Name": "meesho-dev-" + stage}
                for stage in ["bronze", "silver", "gold"]
            ],
            {"Type": "TRIGGER", "Name": "meesho-dev-start"},
        ]
        client.get_workflow.return_value = {
            "Workflow": {"Graph": {"Nodes": nodes}, "MaxConcurrentRuns": 1}
        }
        deploy.verify_workflow(client, "meesho-dev")
        # A deployment must stay paused if a required action is absent.
        nodes.pop(0)
        with self.assertRaisesRegex(RuntimeError, "missing its crawler"):
            deploy.verify_workflow(client, "meesho-dev")

    def test_readiness_rejects_overlapping_workflow_configuration(self):
        client = MagicMock()
        client.get_workflow.return_value = {
            "Workflow": {
                "Graph": {
                    "Nodes": [
                        {"Type": "CRAWLER", "Name": "meesho-dev-raw-discovery"},
                        *[
                            {"Type": "JOB", "Name": "meesho-dev-" + stage}
                            for stage in ["bronze", "silver", "gold"]
                        ],
                    ]
                },
                "MaxConcurrentRuns": 2,
            }
        }
        with self.assertRaisesRegex(RuntimeError, "concurrency is incorrect"):
            deploy.verify_workflow(client, "meesho-dev")

    def test_missing_stack_is_distinct_from_permission_error(self):
        client = MagicMock()
        client.describe_stacks.side_effect = ClientError(
            {"Error": {"Code": "ValidationError", "Message": "Stack does not exist"}},
            "DescribeStacks",
        )
        self.assertIsNone(deploy.describe_stack(client, "meesho-dev-platform"))
        client.describe_stacks.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}},
            "DescribeStacks",
        )
        with self.assertRaises(ClientError):
            deploy.describe_stack(client, "meesho-dev-platform")

    def test_drain_keeps_failed_delivery_paused(self):
        registry = MagicMock()
        registry.get_item.side_effect = [
            {"Item": {"batch_id": "b"}},
            {"Item": {"state": "FAILED"}},
        ]
        with patch.object(deploy, "invoke") as invoke, patch.object(
            deploy, "schedule"
        ) as schedule:
            with self.assertRaisesRegex(RuntimeError, "Resolve delivery b"):
                deploy.drain("meesho-dev", registry)
        self.assertEqual(
            invoke.call_args_list,
            [call("meesho-dev", {"action": "pause"}), call("meesho-dev", {})],
        )
        schedule.assert_called_once_with("meesho-dev", "DISABLED")

    def test_uploader_discovers_environment_bucket_and_submits_manifest_last(self):
        session, cloudformation, s3 = MagicMock(), MagicMock(), MagicMock()
        session.client.side_effect = lambda name: {
            "cloudformation": cloudformation,
            "s3": s3,
        }[name]
        cloudformation.describe_stacks.return_value = {"Stacks": [FOUNDATION]}
        s3.put_object.return_value = {"VersionId": "v1"}
        fixture = str(ROOT / "data/recovery/late-old-version/manifest.json")
        with patch.object(
            upload_delivery.boto3, "Session", return_value=session
        ), patch.object(
            sys,
            "argv",
            ["upload_delivery.py", "--environment", "dev", "--manifest", fixture],
        ):
            upload_delivery.main()
        cloudformation.describe_stacks.assert_called_once_with(
            StackName="meesho-dev-foundation"
        )
        writes = s3.put_object.call_args_list
        self.assertEqual(len(writes), 2)
        self.assertTrue(
            writes[0].kwargs["Key"].startswith("landing/objects/order_item/")
        )
        self.assertTrue(writes[-1].kwargs["Key"].startswith("landing/manifests/"))
        self.assertTrue(all(c.kwargs["Bucket"] == "data-bucket" for c in writes))


if __name__ == "__main__":
    unittest.main()
