"""Bronze job: turn this delivery's JSON lines into accepted event history.

Input: immutable landing files listed in the current manifest; types come from
the crawler's Glue Catalog tables. Output: the bronze.events Iceberg table.
Invalid/conflicting rows are copied to S3 quarantine and excluded. Quality
warnings are recorded while accepted rows continue to Silver.
"""

# F is a short alias for Spark column functions such as col, lit and from_json.
from pyspark.sql import functions as F
from common import Run
from contracts import KEYS


def process(run):
    """Parse this delivery, remove invalid/conflicting events, and merge Bronze."""

    # 1. Collect one Spark DataFrame per entity type present in this delivery.
    # A DataFrame is a table-like collection of rows and columns; frames is a Python list of them.
    frames = []
    # The set removes repeated entity names; sorting gives a predictable processing order.
    for entity in sorted({item["entity"] for item in run.manifest["files"]}):
        # Select only manifest files for this entity, e.g. all payment files in this delivery.
        # The crawler may know other deliveries, but those files are not included here.
        paths = [
            f"s3://{run.bucket}/{item['key']}"
            for item in run.manifest["files"]
            if item["entity"] == entity
        ]
        # Read each JSON line as a string in Spark's value column. Record its source filename.
        raw = run.spark.read.text(paths).withColumn("source_file", F.input_file_name())
        # Parse that string into an event struct using field names/types learned by the crawler.
        parsed = raw.withColumn(
            "event", F.from_json("value", run.source_schema(entity))
        )
        # Select the same event-history columns for every entity so the DataFrames can be combined.
        # F.lit adds a constant to every row; source_entity records the manifest's entity label.
        frames.append(
            parsed.select(
                "value",
                "source_file",
                F.lit(entity).alias("source_entity"),
                "event.event_id",
                "event.entity_type",
                "event.entity_id",
                "event.version",
                F.to_timestamp("event.event_time").alias("event_time"),
                "event.source_system",
                "event.schema_version",
                # Keep the nested business payload as JSON; Silver will expand its fields.
                F.get_json_object("value", "$.payload").alias("payload"),
                # Hash the entire source line. Despite its name, payload_hash also covers event metadata.
                F.sha2("value", 256).alias("payload_hash"),
                # Remember which delivery first inserted this event into Bronze.
                F.lit(run.batch_id).alias("first_batch_id"),
            )
        )

    # 2. Begin with the first DataFrame in the list. Python list positions start at zero.
    # frames[0] means all rows for the first processed entity, not the first data row.
    # Intake accepts only non-empty file manifests, so there is at least one frame.
    events = frames[0]
    # frames[1:] means every remaining DataFrame, starting at the second list position.
    for frame in frames[1:]:
        # Append its rows, matching columns by name. This combines entities; it does not deduplicate.
        events = events.unionByName(frame)

    # 3. Check the event envelope: identifiers, supported entity/version, timestamp and payload.
    # The event's entity label must agree with the manifest folder it came from.
    good = (
        F.col("event_id").isNotNull()
        & F.col("entity_id").isNotNull()
        & F.col("entity_type").isin(list(KEYS))
        & (F.col("entity_type") == F.col("source_entity"))
        & (F.col("version") > 0)
        & (F.col("schema_version") == 1)
        & F.col("event_time").isNotNull()
        & F.col("payload").isNotNull()
        & F.col("source_system").isNotNull()
    )
    # Save invalid rows with their original JSON. coalesce treats a null check as False;
    # ~ reverses the result, so this filter selects rows that failed the check.
    run.quarantine(
        events.filter(~F.coalesce(good, F.lit(False))), "events", "INVALID_ENVELOPE"
    )

    # 4. Compare the manifest's declared total with every input row, including invalid rows.
    expected, actual = (
        sum(item["rows"] for item in run.manifest["files"]),
        events.count(),
    )
    # Save any row-count mismatch as a warning; continue processing the delivery.
    run.quality_result(
        "events",
        "MANIFEST_ROW_COUNT",
        abs(expected - actual),
        {"expected": expected, "actual": actual},
    )
    # Continue with valid envelopes only. The temporary source_entity label is no longer needed.
    events = events.filter(F.coalesce(good, F.lit(False))).drop("source_entity")

    # 5. Find event IDs that occur with different source contents in this delivery.
    # Identical copies have one distinct hash and are safe duplicates.
    conflicts = (
        events.groupBy("event_id")
        .agg(F.countDistinct("payload_hash").alias("n"))
        .filter("n > 1")
        .select("event_id")
    )
    # Copy every conflicting candidate to quarantine; do not choose an arbitrary winner.
    run.quarantine(events.join(conflicts, "event_id"), "events", "CONFLICTING_EVENT_ID")
    # left_anti keeps rows with no matching conflict ID. Then keep one copy of each event ID.
    events = events.join(conflicts, "event_id", "left_anti").dropDuplicates(
        ["event_id"]
    )

    # 6. Compare incoming events with previously accepted Bronze history, when it exists.
    target = run.table("bronze", "events")
    if run.spark.catalog.tableExists(target):
        # s is the incoming DataFrame and t is the stored table. A reused event ID
        # with a different full-line hash must not change the event already accepted.
        conflicts = (
            events.alias("s")
            .join(run.spark.table(target).alias("t"), "event_id")
            .filter(F.col("s.payload_hash") != F.col("t.payload_hash"))
            .select("event_id")
        )
        # Record the conflicting incoming rows, then exclude them from this write.
        run.quarantine(
            events.join(conflicts, "event_id"), "events", "EXISTING_EVENT_CONFLICT"
        )
        events = events.join(conflicts, "event_id", "left_anti")

    # 7. Drop the full raw line from accepted history; landing still preserves it.
    # Cache the accepted DataFrame because quality checks, merge and count reuse it.
    events = events.drop("value").cache()
    # Publish Glue Data Quality results for missing/duplicate event IDs; failures are advisory.
    run.quality(
        events, "events", 'Rules = [IsComplete "event_id", IsUnique "event_id"]'
    )
    # Insert only event IDs not already stored. Replaying identical events adds no duplicate history.
    run.merge(events, "bronze", "events", ["event_id"])
    print(f"Input rows={actual}; accepted unique events={events.count()}")
    # Release the cached rows after the write and counts finish.
    events.unpersist()


# Glue runs this block when it starts bronze.py. Importing the file does not start a job.
if __name__ == "__main__":
    # Load workflow settings, the delivery manifest, AWS clients and a Spark session.
    run = Run("bronze")
    try:
        process(run)
        # Log successful completion after all processing steps return.
        run.finish()
    # Log execution errors and re-raise them so Glue reports FAILED.
    # Recorded data-quality warnings do not raise exceptions here.
    except Exception as error:
        run.fail(error)
        raise
