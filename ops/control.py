"""Operate an already-deployed environment through its coordinator and AWS APIs.

Who uses this file:
    Operators can run these commands for status/recovery. deploy.py imports
    invoke() and schedule() to pause/resume processing, and smoke.py uses the
    command-line actions to register and wait for its acceptance delivery.

Flow: read the requested action -> select the project's resources -> invoke the
coordinator, change its schedule, inspect DynamoDB, or repair manifest notifications.
The coordinator enforces retry/recovery rules; this helper does not bypass them.

Examples: python ops/control.py --project meesho-dev status
          python ops/control.py --project meesho-dev retry --batch BATCH_ID
AWS credentials and Region come from the caller's normal AWS configuration.
"""

import argparse
import json
import time
import boto3
from boto3.dynamodb.conditions import Attr


def invoke(project, payload):
    """Call the project's coordinator synchronously and return its JSON response.

    project is a prefix such as meesho-dev. payload is a Python dictionary,
    for example {"action": "pause"}; an empty dictionary requests a normal tick.
    """
    # Lambda's default RequestResponse invocation waits for the function to finish.
    # Serialize the dictionary to JSON bytes, the payload format accepted by the API.
    response = boto3.client("lambda").invoke(
        FunctionName=project + "-coordinator", Payload=json.dumps(payload).encode()
    )
    # Payload is a readable response stream, not an already-decoded dictionary.
    # Read its bytes and parse the function's returned JSON before using the result.
    body = json.loads(response["Payload"].read())

    # A successful API request can still contain a failed function execution.
    # Check FunctionError so deployment/recovery stops instead of assuming success.
    if response.get("FunctionError"):
        raise RuntimeError(body)
    return body


def schedule(project, state):
    """Set the coordinator schedule to ENABLED/DISABLED while preserving its setup."""
    # 1. Read the existing schedule, including when it runs and which Lambda it invokes.
    client = boto3.client("scheduler")
    existing = client.get_schedule(Name=project + "-dispatch")
    # 2. UpdateSchedule is a replacement-style update, not a change to State alone.
    # Copy the configurable fields; omit read-only response fields such as Arn and dates
    # describing when the schedule resource itself was created/last modified.
    keep = [
        "Name",
        "GroupName",
        "ScheduleExpression",
        "ScheduleExpressionTimezone",
        "StartDate",
        "EndDate",
        "Description",
        "FlexibleTimeWindow",
        "Target",
        "KmsKeyArn",
        "ActionAfterCompletion",
    ]
    # Optional settings may be absent. Copy only fields that the existing schedule has.
    request = {k: existing[k] for k in keep if k in existing}
    request["State"] = state

    # 3. **request expands the dictionary into named API arguments.
    client.update_schedule(**request)


def main():
    """Route one command-line action to the appropriate operation below."""
    # 1. Select the resource prefix and operation. argparse rejects unknown actions.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument(
        "action",
        choices=[
            "status",
            "tick",
            "retry",
            "reject",
            "recover",
            "pause",
            "resume",
            "wait",
            "assert-idle",
            "reconcile-landing",
        ],
    )
    # These inputs are needed only by certain actions; validate them in that branch.
    # --timeout is a waiting limit in seconds, not a Glue job execution timeout.
    parser.add_argument("--batch")
    parser.add_argument("--reason")
    parser.add_argument("--replacement-batch")
    parser.add_argument("--bucket")
    parser.add_argument("--timeout", type=int, default=7200)
    args = parser.parse_args()
    # 2. Bind a DynamoDB Table object to the existing registry. This does not create
    # a table or read its contents; individual branches below make the actual requests.
    table = boto3.resource("dynamodb").Table(args.project + "-registry")

    # Action: pause/resume automatic admissions. The gate controls new deliveries;
    # disabling a schedule alone does not cancel a running workflow or Lambda call.
    if args.action in {"pause", "resume"}:
        if args.action == "pause":
            # Close the gate before disabling regular invocations so no gap admits work.
            invoke(args.project, {"action": "pause"})
            schedule(args.project, "DISABLED")
        else:
            # Restore scheduling while the gate is still closed, then let admissions resume.
            schedule(args.project, "ENABLED")
            invoke(args.project, {"action": "resume"})
    elif args.action == "assert-idle":
        # Action: fail if a delivery still owns the workflow slot. This reads the
        # coordinator's CONTROL record; it does not discover independently started jobs.
        if table.get_item(Key={"pk": "CONTROL"}, ConsistentRead=True).get("Item"):
            raise RuntimeError(
                "A delivery still owns the workflow. Drain/recover it before deployment."
            )
    elif args.action in {"retry", "reject", "recover"}:
        # Action: request a new attempt, reject eligible failed work, or replace a failed
        # delivery with a recovery batch. Reasons/replacement IDs explain the decision.
        if not args.batch or (args.action in {"reject", "recover"} and not args.reason):
            parser.error("--batch is required; reject/recover also require --reason.")
        if args.action == "recover" and not args.replacement_batch:
            parser.error("recover requires --replacement-batch")
        # Send a request to the coordinator, which checks the current state and decides
        # whether the transition is valid. Do not directly overwrite batch/CONTROL items.
        print(
            json.dumps(
                invoke(
                    args.project,
                    {
                        "action": args.action,
                        "batch_id": args.batch,
                        "reason": args.reason,
                        "replacement_batch_id": args.replacement_batch,
                    },
                )
            )
        )
    elif args.action == "tick":
        # Action: run one normal coordinator cycle now. It can update an active delivery
        # or start pending work when admissions are open; this is not a read-only check.
        print(json.dumps(invoke(args.project, {})))
    elif args.action == "wait":
        # Action: actively reconcile a specific batch until it succeeds or needs attention.
        if not args.batch:
            parser.error("wait requires --batch")

        # Use elapsed time so system clock changes cannot extend/shorten the wait.
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            # Refresh execution state through the coordinator. If no work is active,
            # this normal tick may admit a pending batch when the gate is open.
            invoke(args.project, {})

            # BATCH# prefixes distinguish delivery records from CONTROL/MAINTENANCE.
            # A consistent read sees the latest committed update; no Item means intake
            # has not registered this batch yet, which is displayed as AWAITING_INTAKE.
            batch = table.get_item(
                Key={"pk": "BATCH#" + args.batch}, ConsistentRead=True
            ).get("Item", {})
            print(batch.get("state", "AWAITING_INTAKE"), flush=True)
            # Stop waiting on failures, uncertain starts, or an explicitly withdrawn/
            # replaced delivery. These outcomes require a deliberate operator decision.
            if batch.get("state") in {
                "FAILED",
                "NEEDS_REVIEW",
                "REJECTED",
                "SUPERSEDED",
            }:
                raise RuntimeError(json.dumps(batch, default=str))
            # Success also requires this batch to have released the workflow slot.
            # Checking CONTROL avoids returning during the coordinator's final updates.
            owner = table.get_item(Key={"pk": "CONTROL"}, ConsistentRead=True).get(
                "Item", {}
            )
            if (
                batch.get("state") == "SUCCEEDED"
                and owner.get("batch_id") != args.batch
            ):
                # DynamoDB can return Decimal numbers; default=str makes them printable JSON.
                print(json.dumps(batch, default=str, indent=2))
                return

            # Poll every 30 seconds rather than continuously calling AWS APIs.
            time.sleep(30)

        # This timeout stops only the local wait; it does not stop the remote workflow.
        raise TimeoutError(
            "Wait expired; inspect status. The workflow has not been cancelled."
        )
    elif args.action == "reconcile-landing":
        # Action: repair missed intake notifications using manifests already in S3.
        if not args.bucket:
            parser.error("reconcile-landing requires --bucket")
        s3, client = boto3.client("s3"), boto3.client("lambda")
        total = 0
        # 1. Paginate through every manifest under this bucket's landing prefix.
        # Existing batches are recognized by intake, so repeated notifications do not
        # create another batch. This operation does not request a workflow replay.
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=args.bucket, Prefix="landing/manifests/"
        ):
            for item in page.get("Contents", []):
                # 2. Read metadata to include the object's current S3 version ID.
                # Intake needs both key and version to identify the exact manifest object.
                head = s3.head_object(Bucket=args.bucket, Key=item["Key"])
                detail = {
                    "bucket": {"name": args.bucket},
                    "object": {"key": item["Key"], "version-id": head["VersionId"]},
                }
                # 3. Reconstruct the event shape intake normally receives from SQS:
                # Records is a list of messages; each body is JSON containing S3 detail.
                # messageId lets intake identify an individual failed message.
                payload = {
                    "Records": [
                        {
                            "messageId": item["Key"],
                            "body": json.dumps({"detail": detail}),
                        }
                    ]
                }
                # 4. Invoke intake directly and wait for its result. No new SQS message
                # is sent here; we pass the equivalent event payload to the same handler.
                result = client.invoke(
                    FunctionName=args.project + "-intake",
                    Payload=json.dumps(payload).encode(),
                )
                body = json.loads(result["Payload"].read())
                # A handler can report failed messages without a Lambda-level error.
                # Check both failure forms before counting this notification as handled.
                if result.get("FunctionError") or body.get("batchItemFailures"):
                    raise RuntimeError(body)
                total += 1
        # Count notifications processed; an already-registered batch also counts here.
        print(f"Reconciled {total} manifest notifications.")
    else:
        # Action: status. The remaining argparse choice lists existing delivery records.
        # Filter out CONTROL and MAINTENANCE. A scan still reads the table, so this is
        # an operator inspection command rather than the coordinator's dispatch query.
        request = {"FilterExpression": Attr("pk").begins_with("BATCH#")}
        rows = []
        while True:
            page = table.scan(**request)
            # Accumulate each page; one Scan response does not necessarily contain all rows.
            rows.extend(page["Items"])
            if "LastEvaluatedKey" not in page:
                break
            # Continue after the last evaluated item, even if a filtered page was empty.
            request["ExclusiveStartKey"] = page["LastEvaluatedKey"]

        # Scan order is unspecified. Sort by registration time for readable status output.
        print(
            json.dumps(
                sorted(rows, key=lambda x: x["created_at"]), default=str, indent=2
            )
        )


if __name__ == "__main__":
    # Importing invoke()/schedule() from deploy.py does not execute command-line actions.
    main()
