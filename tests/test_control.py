"""Exercise duplicate intake and failure windows without credentials or AWS calls."""

import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"src/lambdas/{name}.py")
    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        os.environ,
        {
            "REGISTRY_TABLE": "registry",
            "DATA_BUCKET": "bucket",
            "WORKFLOW_NAME": "workflow",
            "RELEASE_ID": "test-release",
            "ALERT_TOPIC": "topic",
        },
    ), patch("boto3.client"), patch("boto3.resource"):
        spec.loader.exec_module(module)
    return module


class ControlTests(unittest.TestCase):
    def test_manifest_rejects_path_escape_and_duplicate_files(self):
        mod = load("intake")
        digest = "a" * 64
        entry = {
            "entity": "order_item",
            "key": f"landing/objects/order_item/{digest}.jsonl",
            "sha256": digest,
            "version_id": "v1",
            "rows": 1,
        }
        self.assertEqual(
            mod.validate_manifest({"schema_version": 1, "files": [entry]}), [entry]
        )
        for files in [
            [entry, entry],
            [{**entry, "key": "warehouse/secret"}],
            [{**entry, "rows": 0}],
        ]:
            with self.assertRaises(ValueError):
                mod.validate_manifest({"schema_version": 1, "files": files})

    def test_duplicate_registration_is_acknowledged(self):
        mod = load("intake")
        digest = "a" * 64
        manifest = {
            "schema_version": 1,
            "files": [
                {
                    "entity": "order_item",
                    "key": f"landing/objects/order_item/{digest}.jsonl",
                    "sha256": digest,
                    "version_id": "v1",
                    "rows": 1,
                }
            ],
        }
        mod.s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: json.dumps(manifest).encode())
        }
        mod.s3.head_object.return_value = {
            "VersionId": "v1",
            "Metadata": {"sha256": digest},
        }
        mod.registry.put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
        )
        self.assertEqual(len(mod.register("landing/manifests/m.json", "v1")), 64)

    def test_partial_batch_failure_does_not_retry_success(self):
        mod = load("intake")
        mod.register = MagicMock(side_effect=["ok", ValueError("bad")])

        def record(id):
            return {
                "messageId": id,
                "body": json.dumps(
                    {
                        "detail": {
                            "bucket": {"name": "bucket"},
                            "object": {"key": "landing/manifests/m", "version-id": "v"},
                        }
                    }
                ),
            }

        result = mod.handler({"Records": [record("good"), record("bad")]}, None)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "bad"}]})

    def test_start_failure_retains_claim_and_attempt(self):
        mod = load("coordinator")
        mod.set_state = MagicMock()
        mod.glue.start_workflow_run.side_effect = TimeoutError("unknown result")
        batch = {
            "batch_id": "b",
            "attempt": 0,
            "manifest_key": "m",
            "manifest_version": "v",
        }
        with patch.dict(os.environ, {"RELEASE_ID": "test-release"}), self.assertRaises(
            TimeoutError
        ):
            mod.start(batch)
        mod.registry.delete_item.assert_not_called()
        self.assertEqual(mod.set_state.call_args.args, ("b", "STARTING"))

    def test_claim_before_start_crash_is_recoverable(self):
        mod = load("coordinator")
        batch = {
            "pk": "BATCH#b",
            "batch_id": "b",
            "state": "PENDING",
            "attempt": 0,
            "manifest_key": "m",
            "manifest_version": "v",
        }
        mod.registry.get_item.side_effect = [
            {"Item": {"pk": "CONTROL", "batch_id": "b"}},
            {"Item": batch},
        ]
        mod.start = MagicMock(return_value={"started": True})
        self.assertEqual(mod.handler({}, None), {"started": True})
        mod.start.assert_called_once_with(batch, claim=False)

    def test_unknown_start_is_not_blindly_restarted(self):
        mod = load("coordinator")
        mod.set_state = MagicMock()
        mod.notify = MagicMock()
        mod.glue.get_workflow_runs.return_value = {"Runs": []}
        mod.reconcile(
            {
                "batch_id": "b",
                "state": "STARTING",
                "attempt": 1,
                "started_at": "2020-01-01T00:00:00+00:00",
            }
        )
        mod.glue.start_workflow_run.assert_not_called()
        self.assertEqual(mod.set_state.call_args.args, ("b", "NEEDS_REVIEW"))

    def test_retry_cannot_steal_another_batch_claim(self):
        mod = load("coordinator")
        mod.registry.get_item.side_effect = [
            {"Item": {"batch_id": "other"}},
            {"Item": {"state": "FAILED"}},
        ]
        with self.assertRaises(ValueError):
            mod.handler({"action": "retry", "batch_id": "b"}, None)
        mod.registry.delete_item.assert_not_called()

    def test_reject_is_blocked_after_silver_write_attempt(self):
        mod = load("coordinator")
        mod.registry.get_item.side_effect = [
            {"Item": {"batch_id": "b"}},
            {
                "Item": {
                    "state": "FAILED",
                    "workflow_run_id": "run",
                    "silver_write_started": True,
                }
            },
        ]
        mod.glue.get_workflow_run.return_value = {"Run": {"Status": "COMPLETED"}}
        with self.assertRaises(ValueError):
            mod.handler(
                {"action": "reject", "batch_id": "b", "reason": "bad contract"}, None
            )
        mod.registry.delete_item.assert_not_called()

    def test_recovery_cannot_drop_original_input_files(self):
        mod = load("coordinator")
        original = {
            "state": "FAILED",
            "workflow_run_id": "run",
            "manifest_key": "old",
            "manifest_version": "v1",
        }
        replacement = {
            "state": "PENDING",
            "manifest_key": "new",
            "manifest_version": "v2",
        }
        mod.registry.get_item.side_effect = [
            {"Item": {"batch_id": "b"}},
            {"Item": original},
            {"Item": replacement},
        ]
        mod.glue.get_workflow_run.return_value = {"Run": {"Status": "COMPLETED"}}
        s3 = MagicMock()
        s3.get_object.side_effect = [
            {
                "Body": MagicMock(
                    read=lambda: b'{"files":[{"key":"original","version_id":"v1"}]}'
                )
            },
            {
                "Body": MagicMock(
                    read=lambda: b'{"files":[{"key":"different","version_id":"v2"}]}'
                )
            },
        ]
        with patch.dict(os.environ, {"DATA_BUCKET": "bucket"}), patch(
            "boto3.client", return_value=s3
        ):
            with self.assertRaises(ValueError):
                mod.handler(
                    {
                        "action": "recover",
                        "batch_id": "b",
                        "replacement_batch_id": "new",
                        "reason": "repair",
                    },
                    None,
                )
        mod.registry.put_item.assert_not_called()

    def test_failed_glue_job_does_not_release_a_published_claim(self):
        mod = load("coordinator")
        mod.set_state = MagicMock()
        mod.notify = MagicMock()
        mod.glue.get_workflow_run.return_value = {
            "Run": {
                "Status": "COMPLETED",
                "Graph": {
                    "Nodes": [
                        {
                            "Type": "JOB",
                            "Name": "gold",
                            "JobDetails": {
                                "JobRuns": [
                                    {
                                        "Id": "job",
                                        "JobName": "gold",
                                        "JobRunState": "FAILED",
                                    }
                                ]
                            },
                        }
                    ]
                },
            }
        }
        mod.registry.get_item.side_effect = [
            {"Item": {"state": "SUCCEEDED"}},
        ]
        mod.reconcile({"pk": "BATCH#b", "batch_id": "b", "workflow_run_id": "run"})
        mod.registry.delete_item.assert_not_called()
        self.assertEqual(mod.set_state.call_args.args, ("b", "FAILED"))

    def test_maintenance_gate_blocks_pending_admission(self):
        mod = load("coordinator")
        mod.registry.get_item.side_effect = [{}, {"Item": {"pk": "MAINTENANCE"}}]
        self.assertEqual(mod.handler({}, None), {"state": "PAUSED"})
        mod.registry.query.assert_not_called()
        mod.glue.start_workflow_run.assert_not_called()

    def test_maintenance_still_reconciles_active_delivery(self):
        mod = load("coordinator")
        batch = {"state": "RUNNING", "batch_id": "b"}
        mod.registry.get_item.side_effect = [
            {"Item": {"batch_id": "b"}},
            {"Item": batch},
        ]
        mod.reconcile = MagicMock()
        mod.handler({}, None)
        mod.reconcile.assert_called_once_with(batch)
        mod.registry.query.assert_not_called()

    def test_pause_and_resume_only_change_maintenance_gate(self):
        mod = load("coordinator")
        self.assertEqual(mod.handler({"action": "pause"}, None), {"state": "PAUSED"})
        mod.registry.put_item.assert_called_once_with(Item={"pk": "MAINTENANCE"})
        self.assertEqual(mod.handler({"action": "resume"}, None), {"state": "RESUMED"})
        mod.registry.delete_item.assert_called_once_with(Key={"pk": "MAINTENANCE"})
        mod.glue.start_workflow_run.assert_not_called()

    def test_crawler_failure_is_recorded_and_keeps_claim(self):
        mod = load("coordinator")
        mod.set_state, mod.notify = MagicMock(), MagicMock()
        mod.glue.get_workflow_run.return_value = {
            "Run": {
                "Status": "COMPLETED",
                "Graph": {
                    "Nodes": [
                        {
                            "Type": "CRAWLER",
                            "Name": "raw-discovery",
                            "CrawlerDetails": {
                                "Crawls": [
                                    {"State": "FAILED", "ErrorMessage": "Access denied"}
                                ]
                            },
                        }
                    ]
                },
            }
        }
        mod.registry.get_item.return_value = {"Item": {"state": "RUNNING"}}
        mod.reconcile({"pk": "BATCH#b", "batch_id": "b", "workflow_run_id": "run"})
        mod.registry.delete_item.assert_not_called()
        mod.set_state.assert_called_once_with(
            "b", "FAILED", error="Failed workflow steps: raw-discovery"
        )
        steps = mod.registry.update_item.call_args.kwargs["ExpressionAttributeValues"][
            ":steps"
        ]
        self.assertEqual(steps["raw-discovery"]["error"], "Access denied")
        mod.notify.assert_called_once()

    def test_quality_warning_completion_notifies_and_releases_claim(self):
        mod = load("coordinator")
        mod.notify = MagicMock()
        nodes = [
            {
                "Type": "CRAWLER",
                "Name": "crawler",
                "CrawlerDetails": {"Crawls": [{"State": "SUCCEEDED"}]},
            }
        ]
        nodes += [
            {
                "Type": "JOB",
                "Name": name,
                "JobDetails": {"JobRuns": [{"Id": name, "JobRunState": "SUCCEEDED"}]},
            }
            for name in ["bronze", "silver", "gold"]
        ]
        mod.glue.get_workflow_run.return_value = {
            "Run": {"Status": "COMPLETED", "Graph": {"Nodes": nodes}}
        }
        mod.registry.get_item.return_value = {
            "Item": {
                "state": "SUCCEEDED",
                "publication_id": "run",
                "quality_status": "WARNING",
            }
        }
        mod.reconcile({"pk": "BATCH#b", "batch_id": "b", "workflow_run_id": "run"})
        mod.notify.assert_called_once()
        mod.registry.delete_item.assert_called_once()
        self.assertEqual(
            mod.registry.delete_item.call_args.kwargs["Key"], {"pk": "CONTROL"}
        )

    def test_completed_workflow_without_all_steps_cannot_release_claim(self):
        mod = load("coordinator")
        mod.set_state, mod.notify = MagicMock(), MagicMock()
        mod.glue.get_workflow_run.return_value = {
            "Run": {"Status": "COMPLETED", "Graph": {"Nodes": []}}
        }
        mod.registry.get_item.return_value = {
            "Item": {"state": "SUCCEEDED", "publication_id": "run"}
        }
        mod.reconcile({"pk": "BATCH#b", "batch_id": "b", "workflow_run_id": "run"})
        mod.registry.delete_item.assert_not_called()
        self.assertEqual(mod.set_state.call_args.args, ("b", "FAILED"))


if __name__ == "__main__":
    unittest.main()
