"""Run the project's SQL files or compare Athena totals with an expected dataset.

Who uses this file:
    Operators can execute supplied SQL, and smoke.py uses it to apply views and
    check data/expected.json after the acceptance delivery finishes.

Flow: validate the database prefix -> submit each supplied SQL statement -> wait
for completion -> optionally compare Bronze/Silver counts and Gold financial totals.
Queries use the environment's existing Athena workgroup and its result settings.
This helper returns the first results page; it is intended for supplied SQL and
small verification results, not for exporting an arbitrary large query result.
"""

import argparse
import json
import re
import time
from decimal import Decimal
from pathlib import Path
import boto3


def query(client, workgroup, sql):
    """Run one SQL statement and return its first page of Athena result rows.

    A successful query returns Athena's row/cell dictionaries, including the
    header row for SELECT results. Failure/cancellation raises; timeout cancels it.
    """
    # 1. Submit asynchronously. The response gives an execution ID, not query results.
    # Workgroup settings supply the configured S3 result location and encryption.
    query_id = client.start_query_execution(QueryString=sql, WorkGroup=workgroup)[
        "QueryExecutionId"
    ]
    # 2. Check the same query for up to 600 seconds (10 minutes), using elapsed time.
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        status = client.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]
        if status["State"] == "SUCCEEDED":
            # Results are available only after completion. Return Rows from ResultSet.
            return client.get_query_results(QueryExecutionId=query_id)["ResultSet"][
                "Rows"
            ]
        if status["State"] in {"FAILED", "CANCELLED"}:
            # Preserve Athena's explanation so the caller can find SQL/IAM/data errors.
            raise RuntimeError(status.get("StateChangeReason", status["State"]))

        # QUEUED/RUNNING queries need more time; avoid continuous status requests.
        time.sleep(2)

    # 3. Cancel a query that exceeds this helper's waiting limit, then fail the command.
    client.stop_query_execution(QueryExecutionId=query_id)
    raise TimeoutError(
        "Athena query exceeded 10 minutes and was cancelled: " + query_id
    )


def main():
    """Execute --file SQL and/or --verify dataset checks for the selected environment."""
    # 1. Read the resource prefix, database prefix and optional input file paths.
    # Supplying both --file and --verify executes the SQL first, then checks the data.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--database-prefix", required=True)
    parser.add_argument("--file", type=Path)
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    # 2. Only allow a simple database identifier before inserting it into SQL text.
    # Values such as meesho_dev match; spaces, punctuation and SQL fragments do not.
    if not re.fullmatch("[a-z][a-z0-9_]{2,25}", args.database_prefix):
        parser.error("Invalid database prefix.")
    # Use the caller's AWS credentials/Region and the existing project workgroup.
    client = boto3.client("athena")
    group = args.project + "-analytics"
    db = args.database_prefix
    if args.file:
        # 3. Replace the ${DB} placeholder used by this repository's SQL files.
        # This is plain text substitution after validating the prefix above.
        text = args.file.read_text().replace("${DB}", db)

        # The supplied SQL uses semicolons between statements. This simple split is
        # not a general SQL parser; semicolons inside strings/comments also split text.
        for statement in text.split(";"):
            if statement.strip():
                # Skip empty pieces and finish each statement before submitting the next.
                rows = query(client, group, statement)
                print(json.dumps(rows, indent=2))
    if args.verify:
        # 4. Load acceptance values from JSON, normally data/expected.json.
        # These are totals for the supplied delivery, not targets for unrelated data.
        expected = json.loads(args.verify.read_text())

        # Each pair is (SQL to execute, expected scalar value). Start with Bronze's
        # distinct event count to check that deliberate duplicates were merged.
        checks = [
            (
                f"SELECT count(*) FROM {db}_bronze.events",
                expected["bronze_unique_events"],
            )
        ]
        # Add one count per physical Silver table. Double quotes permit names such
        # as "return" that can otherwise be interpreted as SQL keywords.
        checks.extend(
            (f'SELECT count(*) FROM {db}_silver."{entity}"', count)
            for entity, count in expected["silver_counts"].items()
        )
        # Gold exposes INR amounts, while expected.json stores integer paise.
        # Multiply by 100 and cast to BIGINT so both sides use the same unit.
        # current_* views select the published version rather than all historical rows.
        checks.extend(
            [
                (
                    f"SELECT CAST(sum(net_collected_inr)*100 AS BIGINT) FROM {db}_gold.current_daily_collections",
                    expected["net_collected_paise"],
                ),
                (
                    f"SELECT CAST(sum(booked_value_inr)*100 AS BIGINT) FROM {db}_gold.current_daily_fashion_sales",
                    expected["booked_value_paise"],
                ),
            ]
        )
        # 5. Execute each scalar check and stop immediately if its value differs.
        for sql, wanted in checks:
            result = query(client, group, sql)

            # result[0] is the column-name header; result[1] is the first data row.
            # Data[0] is its first cell. Athena returns that value as VarCharValue text.
            # Decimal converts it to an exact number without binary floating-point rounding.
            actual = Decimal(result[1]["Data"][0]["VarCharValue"])
            if actual != wanted:
                raise AssertionError(f"{sql}: expected {wanted}, got {actual}")
            # Print the verified query and value for the acceptance-run record.
            print(f"PASS: {sql} = {actual}")


if __name__ == "__main__":
    # Run the CLI only for direct execution; importing query() does not submit SQL.
    main()
