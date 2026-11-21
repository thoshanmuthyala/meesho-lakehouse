"""Execute the actual contract predicates and Gold SQL with local Spark 3.5.4.

Requires an optional local PySpark installation and Java. No AWS services are
called. Iceberg commits, Glue DQ and IAM are verified by ops/smoke.py in AWS.
"""

import ast
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from pyspark.sql.readwriter import DataFrameReader
import json
from pathlib import Path
import sys

from pyspark.sql import SparkSession, Window, functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/glue"))
from contracts import CHECKS, KEYS


def constants(path, names):
    """Read the job's actual SQL constants without importing its AWS runtime."""
    values = {}
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in names:
                    values[target.id] = ast.literal_eval(node.value)
    return values


def check_quarantined_delivery(spark, manifest):
    """Execute the actual three job functions with local storage boundaries.

    The committed schema-version-99 fixture must reach Gold as an empty accepted
    input. AWS I/O, Glue DQ and Iceberg transactions are tested separately in AWS;
    here real Spark executes the parsing, filtering, joins and SQL.
    """
    schemas = {}
    for item in manifest["files"]:
        discovered = spark.read.json(str(ROOT / "data/delivery" / item["path"])).schema
        # Exercise Spark's parser for the Hive column types returned by GetTable.
        ddl = ", ".join(
            f"`{field.name}` {field.dataType.simpleString()}" for field in discovered
        )
        schemas[item["entity"]] = spark.createDataFrame([], ddl).schema
    folder = ROOT / "data/recovery/invalid-contract"
    delivery = json.loads((folder / "manifest.json").read_text())
    for item in delivery["files"]:
        item["key"] = str(folder / item["path"])

    class SparkBoundary:
        """Pass Spark operations through; intercept only the Iceberg pointer commit."""

        def __getattr__(self, name):
            return getattr(spark, name)

        def sql(self, query):
            if query.startswith("MERGE INTO probe_gold_publication_current"):
                return None
            return spark.sql(query)

    class LocalRun:
        def __init__(self):
            self.spark, self.bucket = SparkBoundary(), "local"
            self.manifest, self.batch_id, self.run_id = (
                delivery,
                "warning-fixture",
                "local-run",
            )
            self.registry, self.evidence = MagicMock(), []

        def table(self, layer, name):
            return f"probe_{layer}_{name}"

        def source_schema(self, entity):
            return schemas[entity]

        def quarantine(self, rows, dataset, reason):
            self.evidence.append((dataset, reason, rows.count()))

        def publish_views(self):
            # Athena runs only in AWS; the publication call has independent tests.
            pass

        def quality_result(self, *args):
            pass

        def quality(self, *args):
            # The real advisory Glue DQ method has independent offline tests.
            pass

        def merge(self, rows, layer, name, keys, update=False):
            # Materialize each committed stage so later jobs read its actual output.
            rows.localCheckpoint(eager=True).createOrReplaceTempView(
                self.table(layer, name)
            )

    original_text = DataFrameReader.text

    def read_local(reader, paths, *args, **kwargs):
        return original_text(
            reader,
            [path.removeprefix("s3://local/") for path in paths],
            *args,
            **kwargs,
        )

    with patch.dict(sys.modules, {"common": SimpleNamespace(Run=object)}):
        jobs = [importlib.import_module(name) for name in ["bronze", "silver", "gold"]]
    run = LocalRun()
    spark.createDataFrame(
        [("current", "previous")], ["id", "publication_id"]
    ).createOrReplaceTempView(run.table("gold", "publication_current"))
    with patch.object(DataFrameReader, "text", read_local):
        for job in jobs:
            job.process(run)
    assert ("events", "INVALID_ENVELOPE", 1) in run.evidence
    assert spark.table(run.table("bronze", "events")).count() == 0
    assert all(spark.table(run.table("silver", entity)).count() == 0 for entity in KEYS)
    assert (
        run.registry.update_item.call_args.kwargs["ExpressionAttributeValues"][":s"]
        == "SUCCEEDED"
    )
    print(
        "PASS: actual Bronze/Silver/Gold functions complete an all-quarantined delivery."
    )
    # Mix the rejected row with a valid order event. The valid row must survive
    # while absent product/customer references produce advisory Gold violations.
    late_folder = ROOT / "data/recovery/late-old-version"
    late = json.loads((late_folder / "manifest.json").read_text())
    for item in late["files"]:
        item["key"] = str(late_folder / item["path"])
    run.manifest["files"].extend(late["files"])
    run.evidence = []
    with patch.object(DataFrameReader, "text", read_local):
        for job in jobs:
            job.process(run)
    assert spark.table(run.table("bronze", "events")).count() == 1
    assert spark.table(run.table("silver", "order_item")).count() == 1
    assert ("events", "INVALID_ENVELOPE", 1) in run.evidence
    assert ("relationships", "order_customer", 1) in run.evidence
    assert ("relationships", "order_product", 1) in run.evidence
    assert (
        run.registry.update_item.call_args.kwargs["ExpressionAttributeValues"][":s"]
        == "SUCCEEDED"
    )
    print(
        "PASS: mixed valid/invalid input preserves its valid row and publishes despite relationship warnings."
    )


def main():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("meesho-validation")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        manifest = json.loads((ROOT / "data/delivery/manifest.json").read_text())
        expected = json.loads((ROOT / "data/expected.json").read_text())
        paths = [
            str(ROOT / "data/delivery" / item["path"]) for item in manifest["files"]
        ]
        raw = spark.read.text(paths)
        events = (
            raw.select(
                *[
                    F.get_json_object("value", "$." + name).alias(name)
                    for name in [
                        "event_id",
                        "entity_type",
                        "entity_id",
                        "version",
                        "payload",
                    ]
                ]
            )
            .withColumn("version", F.col("version").cast("long"))
            .dropDuplicates(["event_id"])
            .cache()
        )
        assert events.count() == expected["bronze_unique_events"]
        for entity in KEYS:
            # Local-only stand-in for crawler discovery. Deployed jobs obtain
            # the same nested schema through Glue GetTable, never JSON inference.
            entity_paths = [
                str(ROOT / "data/delivery" / item["path"])
                for item in manifest["files"]
                if item["entity"] == entity
            ]
            schema = spark.read.json(entity_paths).schema["payload"].dataType
            rows = (
                events.filter(F.col("entity_type") == entity)
                .select(
                    "entity_id", "version", F.from_json("payload", schema).alias("p")
                )
                .select("entity_id", "version", "p.*")
            )
            invalid = rows.filter(
                ~F.coalesce(F.expr(CHECKS[entity]), F.lit(False))
            ).count()
            assert invalid == 0, (entity, "contract failures", invalid)
            latest = (
                rows.withColumn(
                    "rank",
                    F.row_number().over(
                        Window.partitionBy("entity_id").orderBy(F.desc("version"))
                    ),
                )
                .filter("rank=1")
                .drop("rank")
                .cache()
            )
            assert latest.count() == expected["silver_counts"][entity]
            latest.createOrReplaceTempView("returns" if entity == "return" else entity)
        sql = constants(ROOT / "src/glue/gold.py", {"RELATIONSHIPS", "MARTS"})
        for name, statement in sql["RELATIONSHIPS"].items():
            assert spark.sql(statement).count() == 0, ("relationship failures", name)
        for name, (_, statement) in sql["MARTS"].items():
            frame = spark.sql(statement).cache()
            assert frame.count() > 0, ("empty mart", name)
            frame.createOrReplaceTempView(name)
        cash = spark.sql(
            "SELECT sum(net_collected_inr) FROM daily_collections"
        ).first()[0]
        booked = spark.sql(
            "SELECT sum(booked_value_inr) FROM daily_fashion_sales"
        ).first()[0]
        assert int(cash * 100) == expected["net_collected_paise"]
        assert int(booked * 100) == expected["booked_value_paise"]
        print(
            "PASS: Spark contracts, latest-version selection, relationships and all four Gold queries."
        )
        print(f"PASS: net collections INR {cash}; booked value INR {booked}.")
        spark.catalog.clearCache()
        check_quarantined_delivery(spark, manifest)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
