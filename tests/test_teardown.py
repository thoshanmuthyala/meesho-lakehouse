"""Exercise deletion boundaries without deleting or connecting to AWS resources.

Load the actual inline Lambda from bootstrap.yaml so tests cannot drift from the
code that CloudFormation deploys. All service clients and callbacks are mocked.
"""

import json
import os
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError
from test_environments import ROOT, template


def aws_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "TestOperation")


class TeardownTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bootstrap = template("bootstrap.yaml")
        cls.source = cls.bootstrap["Resources"]["CleanupFunction"]["Properties"][
            "Code"
        ]["ZipFile"]

    def setUp(self):
        self.response = SimpleNamespace(
            SUCCESS="SUCCESS", FAILED="FAILED", send=MagicMock()
        )
        self.cleanup = ModuleType("cleanup_under_test")
        with patch.dict(sys.modules, {"cfnresponse": self.response}), patch.dict(
            os.environ,
            {
                "PROJECT": "meesho-dev",
                "ACCOUNT": "123456789012",
                "AWS_REGION": "us-east-1",
            },
        ):
            exec(
                compile(self.source, "bootstrap-inline-cleanup.py", "exec"),
                self.cleanup.__dict__,
            )
        self.clients = {
            name: MagicMock()
            for name in (
                "s3",
                "cloudformation",
                "lambda",
                "glue",
                "logs",
                "dynamodb",
                "codebuild",
            )
        }
        self.cleanup.client = self.clients.__getitem__
        self.context = MagicMock()
        self.context.get_remaining_time_in_millis.return_value = 800000
        self.bucket = "meesho-dev-data-123456789012-us-east-1"
        self.event = {
            "RequestType": "Delete",
            "StackId": "stack-id",
            "LogicalResourceId": "DataCleanup",
            "PhysicalResourceId": "stable-resource-id",
            "ResourceProperties": {"Mode": "Data", "Bucket": self.bucket},
        }
        self.clients["cloudformation"].describe_stacks.return_value = {
            "Stacks": [{"StackStatus": "DELETE_IN_PROGRESS"}]
        }

    def ready_bucket(self):
        s3 = self.clients["s3"]
        s3.get_bucket_policy.return_value = {
            "Policy": json.dumps(
                {"Version": "2012-10-17", "Statement": [{"Sid": "ExistingRule"}]}
            )
        }
        s3.list_multipart_uploads.return_value = {}
        s3.list_object_versions.return_value = {}
        s3.delete_objects.return_value = {}
        return s3

    def ready_platform(self):
        self.clients["lambda"].invoke.return_value = {}
        glue = self.clients["glue"]
        glue.get_workflow_runs.return_value = {"Runs": [{"Status": "COMPLETED"}]}
        glue.get_crawler.return_value = {"Crawler": {"State": "READY"}}
        glue.get_job_runs.return_value = {"JobRuns": []}
        glue.list_crawls.return_value = {"Crawls": []}
        self.clients["dynamodb"].describe_continuous_backups.return_value = {
            "ContinuousBackupsDescription": {
                "PointInTimeRecoveryDescription": {
                    "PointInTimeRecoveryStatus": "DISABLED"
                }
            }
        }
        return glue

    def test_every_resource_is_deleted_and_templates_remain_console_uploadable(self):
        for name in ("bootstrap.yaml", "foundation.yaml", "platform.yaml"):
            self.assertLess((ROOT / "cloudformation" / name).stat().st_size, 51200)
            for logical_id, resource in template(name)["Resources"].items():
                with self.subTest(template=name, resource=logical_id):
                    self.assertEqual(resource["DeletionPolicy"], "Delete")
                    self.assertEqual(resource["UpdateReplacePolicy"], "Delete")
        self.assertEqual(
            self.bootstrap["Resources"]["DataKey"]["Properties"]["PendingWindowInDays"],
            7,
        )
        self.assertTrue(
            template("platform.yaml")["Resources"]["Athena"]["Properties"][
                "RecursiveDeleteOption"
            ]
        )

    def test_cross_stack_imports_and_internal_dependencies_enforce_deletion_order(self):
        foundation = template("foundation.yaml")
        platform = template("platform.yaml")
        exported_helper = self.bootstrap["Outputs"]["CleanupServiceToken"]["Export"][
            "Name"
        ]["Sub"]
        self.assertEqual(exported_helper, "meesho-${Environment}-cleanup-service-token")
        for stack, resource in (
            (foundation, "DataCleanup"),
            (platform, "PlatformCleanup"),
        ):
            self.assertEqual(
                stack["Resources"][resource]["Properties"]["ServiceToken"][
                    "Fn::ImportValue"
                ]["Sub"],
                "${Project}-cleanup-service-token",
            )
        self.assertEqual(
            platform["Resources"]["PlatformCleanup"]["Properties"]["Bucket"][
                "Fn::ImportValue"
            ],
            foundation["Outputs"]["DataBucket"]["Export"]["Name"],
        )
        dependencies = set(platform["Resources"]["PlatformCleanup"]["DependsOn"])
        self.assertEqual(
            dependencies,
            set(platform["Resources"]) - {"PlatformCleanup", "AlertSubscription"},
        )
        self.assertEqual(
            foundation["Resources"]["DataCleanup"]["DependsOn"], "DataPolicy"
        )

    def test_job_logs_are_owned_by_the_environment_stack(self):
        resources = template("platform.yaml")["Resources"]
        for stage in ("Bronze", "Silver", "Gold"):
            job = resources[stage + "Job"]
            self.assertEqual(job["DependsOn"], ["GlueErrorLogs", "GlueOutputLogs"])
            self.assertEqual(
                job["Properties"]["DefaultArguments"]["--custom-logGroup-prefix"],
                {"Sub": "/aws-glue/${Project}"},
            )
        for name in ("Intake", "Coordinator"):
            self.assertEqual(resources[name]["DependsOn"], name + "Logs")

    def test_create_and_update_never_access_aws_or_change_the_physical_id(self):
        for request in ("Create", "Update"):
            self.cleanup.handler({**self.event, "RequestType": request}, self.context)
            self.assertEqual(self.response.send.call_args.args[2], "SUCCESS")
            self.assertEqual(self.response.send.call_args.args[4], "stable-resource-id")
        for service in self.clients.values():
            self.assertEqual(service.mock_calls, [])

    def test_failed_update_rollback_does_not_empty_an_existing_bucket(self):
        for status in (
            "UPDATE_IN_PROGRESS",
            "UPDATE_ROLLBACK_IN_PROGRESS",
            "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
        ):
            self.clients["cloudformation"].describe_stacks.return_value = {
                "Stacks": [{"StackStatus": status}]
            }
            self.cleanup.handler(self.event, self.context)
            self.assertEqual(self.response.send.call_args.args[2], "SUCCESS")
        self.assertEqual(self.clients["s3"].mock_calls, [])

    def test_bucket_cleanup_deletes_every_version_marker_and_multipart_page(self):
        s3 = self.ready_bucket()
        s3.list_multipart_uploads.side_effect = [
            {"Uploads": [{"Key": "part", "UploadId": "u1"}]},
            {"Uploads": [{"Key": "part2", "UploadId": "u2"}]},
            {},
        ]
        s3.list_object_versions.side_effect = [
            {
                "Versions": [{"Key": "orders", "VersionId": "v1"}],
                "DeleteMarkers": [{"Key": "orders", "VersionId": "d1"}],
                "IsTruncated": True,
            },
            {"Versions": [{"Key": "orders", "VersionId": "v2"}]},
            {},
        ]
        self.cleanup.handler(self.event, self.context)
        self.assertEqual(self.response.send.call_args.args[2], "SUCCESS")
        self.assertEqual(s3.abort_multipart_upload.call_count, 2)
        self.assertEqual(
            s3.delete_objects.call_args_list[0].kwargs["Delete"]["Objects"],
            [
                {"Key": "orders", "VersionId": "v1"},
                {"Key": "orders", "VersionId": "d1"},
            ],
        )
        self.assertEqual(s3.delete_objects.call_count, 2)
        policy = json.loads(s3.put_bucket_policy.call_args.kwargs["Policy"])
        self.assertEqual(policy["Statement"][0], {"Sid": "ExistingRule"})
        self.assertEqual(policy["Statement"][1]["Action"], "s3:PutObject")

    def test_other_environment_or_account_buckets_are_rejected_before_access(self):
        for bucket in (
            self.bucket.replace("-dev-", "-prod-"),
            self.bucket.replace("123456789012", "999999999999"),
            "unrelated-bucket",
        ):
            with self.assertRaises(ValueError):
                self.cleanup.empty_bucket(bucket, self.context)
        self.assertEqual(self.clients["s3"].mock_calls, [])

    def test_partial_s3_errors_and_time_budget_report_failure(self):
        s3 = self.ready_bucket()
        s3.list_object_versions.return_value = {
            "Versions": [{"Key": "orders", "VersionId": "v1"}]
        }
        s3.delete_objects.return_value = {"Errors": [{"Code": "AccessDenied"}]}
        self.cleanup.handler(self.event, self.context)
        self.assertEqual(self.response.send.call_args.args[2], "FAILED")
        self.context.get_remaining_time_in_millis.return_value = 1000
        self.cleanup.handler(self.event, self.context)
        self.assertEqual(self.response.send.call_args.args[2], "FAILED")
        self.assertIn("Retry Delete", self.response.send.call_args.kwargs["reason"])

    def test_missing_bucket_is_retry_safe_but_access_denied_is_not_ignored(self):
        for error, status in (("NoSuchBucket", "SUCCESS"), ("AccessDenied", "FAILED")):
            self.clients["s3"].get_bucket_policy.side_effect = aws_error(error)
            self.cleanup.handler(self.event, self.context)
            self.assertEqual(self.response.send.call_args.args[2], status)

    def test_active_workflow_or_manual_job_prevents_destructive_platform_cleanup(self):
        glue = self.ready_platform()
        glue.get_workflow_runs.return_value = {"Runs": [{"Status": "RUNNING"}]}
        with self.assertRaisesRegex(RuntimeError, "active Glue workflow"):
            self.cleanup.prepare_platform(self.context)
        self.clients["dynamodb"].update_continuous_backups.assert_not_called()
        glue.get_workflow_runs.return_value = {"Runs": []}
        glue.get_job_runs.return_value = {
            "JobRuns": [{"Id": "jr_busy", "JobRunState": "WAITING"}]
        }
        with self.assertRaisesRegex(RuntimeError, "active Glue jobs"):
            self.cleanup.prepare_platform(self.context)
        self.clients["logs"].delete_log_stream.assert_not_called()
        self.clients["dynamodb"].update_continuous_backups.assert_not_called()

    def test_pitr_disabled_and_only_history_identified_crawler_streams_deleted(self):
        glue = self.ready_platform()
        glue.list_crawls.side_effect = [
            {
                "Crawls": [
                    {
                        "LogGroup": "/aws-glue/crawlers",
                        "LogStream": "meesho-dev-raw-discovery",
                    }
                ],
                "NextToken": "page2",
            },
            {
                "Crawls": [
                    {
                        "LogGroup": "/aws-glue/crawlers",
                        "LogStream": "meesho-dev-raw-discovery",
                    }
                ]
            },
        ]
        self.cleanup.prepare_platform(self.context)
        self.clients["dynamodb"].update_continuous_backups.assert_called_once_with(
            TableName="meesho-dev-registry",
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False},
        )
        self.clients["logs"].delete_log_stream.assert_called_once_with(
            logGroupName="/aws-glue/crawlers", logStreamName="meesho-dev-raw-discovery"
        )
        self.clients["logs"].delete_log_group.assert_not_called()
        self.assertEqual(glue.list_crawls.call_args.kwargs["NextToken"], "page2")

    def test_legacy_glue_logs_match_run_boundary_and_preserve_shared_groups(self):
        glue = self.ready_platform()
        glue.get_job_runs.side_effect = [
            {"JobRuns": [{"Id": "jr_abc", "JobRunState": "SUCCEEDED"}]},
            {"JobRuns": []},
            {"JobRuns": []},
        ]
        self.clients["logs"].describe_log_streams.return_value = {
            "logStreams": [
                {"logStreamName": "jr_abc-driver"},
                {"logStreamName": "jr_abcdef-driver"},
            ]
        }
        self.cleanup.prepare_platform(self.context)
        for deletion in self.clients["logs"].delete_log_stream.call_args_list:
            self.assertEqual(deletion.kwargs["logStreamName"], "jr_abc-driver")
        self.assertEqual(self.clients["logs"].delete_log_stream.call_count, 3)
        self.clients["logs"].delete_log_group.assert_not_called()

    def test_active_build_blocks_history_and_log_deletion(self):
        build = self.clients["codebuild"]
        build.list_builds_for_project.return_value = {"ids": ["meesho-dev-deploy:one"]}
        build.batch_get_builds.return_value = {"builds": [{"buildComplete": False}]}
        with self.assertRaisesRegex(RuntimeError, "active CodeBuild"):
            self.cleanup.clean_builds(self.context)
        build.batch_delete_builds.assert_not_called()
        self.clients["logs"].delete_log_group.assert_not_called()

    def test_build_history_pagination_and_partial_failure(self):
        build = self.clients["codebuild"]
        build.list_builds_for_project.side_effect = [
            {"ids": ["meesho-dev-validate:one"], "nextToken": "page2"},
            {"ids": ["meesho-dev-validate:two"]},
            {"ids": []},
        ]
        build.batch_get_builds.return_value = {"builds": [{"buildComplete": True}]}
        build.batch_delete_builds.return_value = {
            "buildsNotDeleted": [{"statusCode": "BUILD_IN_PROGRESS"}]
        }
        with self.assertRaisesRegex(RuntimeError, "history cleanup incomplete"):
            self.cleanup.clean_builds(self.context)
        self.assertEqual(
            build.list_builds_for_project.call_args_list[1],
            call(projectName="meesho-dev-validate", nextToken="page2"),
        )
        self.clients["logs"].delete_log_group.assert_not_called()


if __name__ == "__main__":
    unittest.main()
