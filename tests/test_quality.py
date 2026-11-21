"""Check advisory DQ and Catalog consumption without Glue libraries or AWS calls."""

import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]


def common_module():
    # Glue supplies these modules in AWS. Mock their boundaries, not the Run
    # methods under test, so offline CI does not require a Spark installation.
    names = [
        "awsglue",
        "awsglue.context",
        "awsglue.dynamicframe",
        "awsglue.utils",
        "awsgluedq",
        "awsgluedq.transforms",
        "pyspark",
        "pyspark.context",
        "pyspark.sql",
    ]
    spec = importlib.util.spec_from_file_location(
        "quality_common", ROOT / "src/glue/common.py"
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {name: MagicMock() for name in names}):
        spec.loader.exec_module(module)
    return module


class QualityTests(unittest.TestCase):
    def test_failed_rule_publishes_warning_without_raising(self):
        module = common_module()
        run = module.Run.__new__(module.Run)
        run.bucket, run.batch_id, run.run_id, run.stage = (
            "bucket",
            "batch",
            "run",
            "silver",
        )
        run.s3, run.registry, run.context = MagicMock(), MagicMock(), MagicMock()
        failed = MagicMock()
        failed.asDict.return_value = {"Rule": "IsUnique id", "Outcome": "Failed"}
        module.EvaluateDataQuality.apply.return_value.toDF.return_value.collect.return_value = [
            failed
        ]
        frame = MagicMock()
        frame.limit.return_value.count.return_value = 2
        run.quality(frame, "order_item", 'Rules = [IsUnique "id"]')
        self.assertIn(
            b'"outcome": "WARNING"', run.s3.put_object.call_args.kwargs["Body"]
        )
        self.assertEqual(
            run.registry.update_item.call_args.kwargs["ExpressionAttributeValues"][
                ":warning"
            ],
            "WARNING",
        )

    def test_empty_dataset_records_warning_without_column_analyzers(self):
        module = common_module()
        run = module.Run.__new__(module.Run)
        run.quality_result = MagicMock()
        frame = MagicMock()
        frame.limit.return_value.count.return_value = 0
        run.quality(frame, "events", 'Rules = [IsUnique "event_id"]')
        module.EvaluateDataQuality.apply.assert_not_called()
        run.quality_result.assert_called_once_with("events", "EMPTY_DATASET", 1)

    def test_evidence_write_error_is_not_hidden_as_quality_warning(self):
        module = common_module()
        run = module.Run.__new__(module.Run)
        run.bucket, run.batch_id, run.run_id, run.stage = (
            "bucket",
            "batch",
            "run",
            "bronze",
        )
        run.s3, run.registry = MagicMock(), MagicMock()
        run.s3.put_object.side_effect = PermissionError("Access denied")
        with self.assertRaises(PermissionError):
            run.quality_result("events", "INVALID_ENVELOPE", 1)
        run.registry.update_item.assert_not_called()

    def test_athena_views_are_created_automatically_and_failures_surface(self):
        module = common_module()
        run = module.Run.__new__(module.Run)
        run.prefix = "meesho_dev"
        module.getResolvedOptions.return_value = {
            "ATHENA_WORKGROUP": "meesho-dev-analytics"
        }
        client = MagicMock()
        client.start_query_execution.return_value = {"QueryExecutionId": "q"}
        client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        with patch.object(module.boto3, "client", return_value=client), patch.object(
            module.Path,
            "read_text",
            return_value="CREATE VIEW ${DB}_gold.v AS SELECT 1;",
        ):
            run.publish_views()
            client.start_query_execution.assert_called_once_with(
                QueryString="CREATE VIEW meesho_dev_gold.v AS SELECT 1",
                WorkGroup="meesho-dev-analytics",
            )
            client.get_query_execution.return_value = {
                "QueryExecution": {
                    "Status": {"State": "FAILED", "StateChangeReason": "Access denied"}
                }
            }
            with self.assertRaisesRegex(RuntimeError, "Access denied"):
                run.publish_views()

    def test_schema_comes_from_catalog_including_unlisted_payload_fields(self):
        module = common_module()
        run = module.Run.__new__(module.Run)
        run.prefix, run.schemas = "meesho_dev", {}
        run.glue, run.spark = MagicMock(), MagicMock()
        run.glue.get_table.return_value = {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "event_id", "Type": "string"},
                        {
                            "Name": "payload",
                            "Type": "struct<product_id:string,new_field:bigint>",
                        },
                    ]
                }
            }
        }
        schema = run.source_schema("product")
        self.assertIs(run.source_schema("product"), schema)
        run.glue.get_table.assert_called_once_with(
            DatabaseName="meesho_dev_raw", Name="discovered_product"
        )
        run.spark.createDataFrame.assert_called_once_with(
            [],
            "`event_id` string, `payload` struct<product_id:string,new_field:bigint>",
        )


if __name__ == "__main__":
    unittest.main()
