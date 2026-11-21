"""Shared setup and storage helpers used by Bronze, Silver and Gold.

Run loads the delivery manifest and creates Spark/AWS clients. Its methods read
Catalog schemas, save quality/quarantine evidence, merge Iceberg tables, and
prepare Athena views. The job files call these helpers in their processing order.
Quality findings are advisory; errors saving data or calling AWS still propagate.
"""

import json
import sys
import time
from pathlib import Path

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
# F is a short alias for Spark column functions such as col, lit and from_json.
from pyspark.sql import functions as F


class Run:
    """Settings and shared services for one Bronze, Silver or Gold job attempt."""

    def __init__(self, stage):

        # 1. Read job arguments supplied by Glue/workflow and platform.yaml.
        # These select the environment and current workflow run; no local exports are required.
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "WORKFLOW_NAME",
                "WORKFLOW_RUN_ID",
                "DATA_BUCKET",
                "REGISTRY_TABLE",
                "DATABASE_PREFIX",
                "RELEASE_ID",
            ],
        )
        self.args, self.stage = args, stage

        # 2. Create clients using the Glue job's IAM role. The role supplies AWS credentials.
        self.glue = boto3.client("glue")
        self.s3 = boto3.client("s3")
        self.registry = boto3.resource("dynamodb").Table(args["REGISTRY_TABLE"])
        # Keep commonly used resource names and the workflow run ID on this Run object.
        self.bucket = args["DATA_BUCKET"]
        self.prefix = args["DATABASE_PREFIX"]
        self.run_id = args["WORKFLOW_RUN_ID"]

        # 3. Read the batch ID and manifest location set by coordinator when it started the workflow.
        props = self.glue.get_workflow_run_properties(
            Name=args["WORKFLOW_NAME"], RunId=self.run_id
        )["RunProperties"]
        self.batch_id = props["batch_id"]
        # Read that exact S3 manifest version. Its files list defines what this attempt processes.
        self.manifest = json.loads(
            self.s3.get_object(
                Bucket=self.bucket,
                Key=props["manifest_key"],
                VersionId=props["manifest_version"],
            )["Body"].read()
        )

        # 4. Reuse/create Spark and wrap it in GlueContext for Glue Data Quality integration.
        self.context = GlueContext(SparkContext.getOrCreate())
        self.spark = self.context.spark_session
        # Parse source timestamps consistently in UTC; Gold converts reporting dates to Indian time.
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
        # Cache discovered schemas here so repeated requests do not repeat the Glue GetTable call.
        self.schemas = {}
        print(f"Starting {stage}: batch={self.batch_id}, run={self.run_id}")

    def table(self, layer, name):
        """Build a Spark name such as glue_catalog.meesho_dev_silver.product."""
        # glue_catalog is configured in the job's Spark settings; the database prefix selects dev/prod.
        return f"glue_catalog.{self.prefix}_{layer}.{name}"

    def source_schema(self, entity):
        """Return the crawler's schema for Bronze parsing and Silver payload fields."""
        # Fetch each entity schema once per job; reuse the saved schema on later calls.
        if entity not in self.schemas:
            table = self.glue.get_table(
                DatabaseName=f"{self.prefix}_raw", Name=f"discovered_{entity}"
            )["Table"]
            # These column names and types were discovered by the crawler, including nested payload fields.
            columns = table["StorageDescriptor"]["Columns"]
            # Build a Spark schema description from Catalog metadata, e.g. `version` bigint.
            # Backticks quote column names; doubling an embedded backtick escapes it.
            ddl = ", ".join(
                f"`{column['Name'].replace('`', '``')}` {column['Type']}"
                for column in columns
            )
            # Create an empty DataFrame only to let Spark parse the schema text into a schema object.
            # This does not create source data or scan the raw Catalog table.
            self.schemas[entity] = self.spark.createDataFrame([], ddl).schema
        return self.schemas[entity]

    def quality_result(self, dataset, check, count, details=None):
        """Save a check in S3 and mark the batch WARNING when its affected count is positive."""

        # 1. Build one diagnostic result. A positive affected count means WARNING; zero means PASSED.
        result = dict(
            dataset=dataset,
            check=check,
            affected_count=count,
            outcome="WARNING" if count else "PASSED",
            details=details or {},
        )

        # 2. Include batch, workflow attempt, stage, dataset and check in the S3 path.
        # A retry has a new workflow run ID, so its results do not overwrite the previous attempt.
        key = (
            f"quality/{self.batch_id}/{self.run_id}/{self.stage}/{dataset}/{check}.json"
        )
        # Save the result as JSON. If saving fails, surface that execution error to Glue.
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=(json.dumps(result, default=str) + "\n").encode(),
            ContentType="application/json",
        )
        print(json.dumps(result, default=str))

        # 3. For a warning, increment the batch's failed-check counter in DynamoDB once.
        # This counts failed checks, not the number of rejected records.
        if count:
            self.registry.update_item(
                Key={"pk": "BATCH#" + self.batch_id},
                UpdateExpression="SET quality_status=:warning ADD quality_warning_count :one",
                ExpressionAttributeValues={":warning": "WARNING", ":one": 1},
            )

    def quarantine(self, rows, dataset, reason):
        """Save problem rows and their reason for this workflow attempt."""
        # Count the supplied problem rows; do not create empty quarantine files.
        count = rows.count()
        if count:
            # Add the reason to each diagnostic row and write it under this attempt's S3 path.
            # Bronze/Silver callers exclude these rows; Gold callers keep Silver unchanged.
            rows.withColumn("reason", F.lit(reason)).write.mode("overwrite").json(
                f"s3://{self.bucket}/quarantine/{self.batch_id}/{self.run_id}/"
                f"{self.stage}/{dataset}/{reason}/"
            )
        # Always save the count, including zero, so a completed check has visible evidence.
        self.quality_result(dataset, reason, count)

    def quality(self, df, dataset, rules):
        """Publish Glue DQ results without raising when a rule reports failure."""

        # 1. Check for at least one accepted row. Empty input gets a warning and continues;
        # column analyzers that require data are skipped for this case.
        if not df.limit(1).count():
            self.quality_result(dataset, "EMPTY_DATASET", 1)
            return

        # 2. Convert the Spark DataFrame into the Glue DynamicFrame expected by this DQ transform.
        frame = DynamicFrame.fromDF(df, self.context, dataset)

        # 3. Evaluate the caller's rules and publish results/metrics to Glue, S3 and CloudWatch.
        # The returned DataFrame contains rule outcomes, not rejected business rows.
        result = EvaluateDataQuality.apply(
            frame=frame,
            ruleset=rules,
            publishing_options={
                "dataQualityEvaluationContext": f"{self.stage}_{dataset}",
                "enableDataQualityCloudWatchMetrics": True,
                "enableDataQualityResultsPublishing": True,
                "resultsS3Prefix": f"s3://{self.bucket}/quality/{self.batch_id}/{self.run_id}/glue/",
            },
        ).toDF()

        # 4. Collect the small rule-result set into the Python process coordinating Spark.
        # Convert its rows to dictionaries for JSON output.
        outcomes = [row.asDict(recursive=True) for row in result.collect()]
        # Save how many rules did not pass, plus each rule's details. Do not raise for failed rules.
        self.quality_result(
            dataset,
            "GLUE_DQ",
            sum(row["Outcome"] != "Passed" for row in outcomes),
            {"rules": outcomes},
        )

    def merge(self, df, layer, name, keys, update=False):
        """Create/merge an Iceberg table; Silver updates require a higher source version."""

        # 1. Resolve the destination table for this environment and layer.
        target = self.table(layer, name)

        # 2. On the first write, create an empty Iceberg table with the incoming DataFrame's columns.
        # Later deliveries use the existing table. Schema migration is a separate reviewed change.
        if not self.spark.catalog.tableExists(target):
            # limit(0) carries the schema without data. Format v2 supports these merge operations;
            # location selects S3 storage, and the target data-file size is 128 MiB.
            writer = (
                df.limit(0)
                .writeTo(target)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .tableProperty(
                    "location", f"s3://{self.bucket}/warehouse/{layer}/{name}"
                )
                .tableProperty("write.target-file-size-bytes", "134217728")
            )
            # Organize Bronze files by event day for history queries; Silver/Gold use no partition here.
            if layer == "bronze":
                writer = writer.partitionedBy(F.days("event_time"))
            writer.create()

        # 3. Give incoming rows a temporary SQL name in this Spark session; this is not an Athena view.
        df.createOrReplaceTempView("incoming_rows")
        # Build the match condition from business keys. t means target table; s means source rows.
        match = " AND ".join(f"t.{key} = s.{key}" for key in keys)
        # Only Silver passes update=True: replace a matched row only for a higher source version.
        # Bronze and Gold preserve existing matches and insert previously unseen keys.
        update_sql = (
            "WHEN MATCHED AND s.version > t.version THEN UPDATE SET *" if update else ""
        )

        # 4. Commit this table through Iceberg MERGE. Atomicity applies to this table only;
        # Gold uses a publication pointer to coordinate visibility across its four reporting tables.
        self.spark.sql(
            f"MERGE INTO {target} t USING incoming_rows s ON {match} "
            f"{update_sql} WHEN NOT MATCHED THEN INSERT *"
        )
        print(f"Committed {layer}.{name}: batch={self.batch_id}")

    def publish_views(self):
        """Let Gold create or refresh Athena views after the underlying tables exist."""

        # 1. Gold alone receives the Athena workgroup argument and calls this method.
        options = getResolvedOptions(sys.argv, ["ATHENA_WORKGROUP"])
        client = boto3.client("athena")

        # 2. Read the release SQL copied by Glue --extra-files and fill in this environment's prefix.
        sql = Path("views.sql").read_text().replace("${DB}", self.prefix)

        # 3. Run the simple semicolon-separated CREATE VIEW statements one at a time.
        for statement in sql.split(";"):
            if not statement.strip():
                continue
            # Athena runs asynchronously; the workgroup supplies the encrypted query-output location.
            query_id = client.start_query_execution(
                QueryString=statement, WorkGroup=options["ATHENA_WORKGROUP"]
            )["QueryExecutionId"]

            # 4. Wait up to five minutes for this query, checking its state every two seconds.
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline:
                status = client.get_query_execution(QueryExecutionId=query_id)[
                    "QueryExecution"
                ]["Status"]
                if status["State"] == "SUCCEEDED":
                    break
                # A failed SQL query is an execution failure, so stop Gold with Athena's error message.
                if status["State"] in {"FAILED", "CANCELLED"}:
                    raise RuntimeError(status.get("StateChangeReason", status["State"]))
                time.sleep(2)
            else:
                # The polling loop expired: cancel the outstanding query and report a timeout.
                client.stop_query_execution(QueryExecutionId=query_id)
                raise TimeoutError("Athena view creation timed out: " + query_id)
        print("Athena current-publication views are ready.")

    def finish(self):
        """Detailed diagnostics remain in the normal Glue CloudWatch log stream."""
        # Glue determines final job status from script completion; this method writes a log message.
        print(f"Succeeded {self.stage}: run={self.run_id}")

    def fail(self, error):
        """Execution errors propagate; the coordinator reads actual Glue status."""
        # The job entry point re-raises the error after this log. Coordinator later reads Glue's status.
        print(f"Failed {self.stage}: {error}")
