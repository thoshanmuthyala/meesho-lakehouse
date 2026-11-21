"""One coordinator owns admission and recovery for one Glue workflow.

Reserved Lambda concurrency and workflow concurrency are both 1. CONTROL keeps
ownership across invocations. Failed/ambiguous work stays visible and blocks new
batches; operators retry the same manifest after correcting the root cause.
"""

import json
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key

registry = boto3.resource("dynamodb").Table(os.environ["REGISTRY_TABLE"])
glue, sns = boto3.client("glue"), boto3.client("sns")
WORKFLOW = os.environ["WORKFLOW_NAME"]
TERMINAL_FAILURES = {"FAILED", "TIMEOUT", "STOPPED", "ERROR", "EXPIRED", "CANCELLED"}


def now():
    """Store UTC timestamps consistently across Lambda invocations."""
    return datetime.now(timezone.utc).isoformat()


def set_state(batch_id, state, **fields):
    """Update only named attributes; preserve the immutable manifest reference."""
    values = {":state": state}
    names = {"#state": "state"}
    updates = ["#state=:state"]
    for index, (key, value) in enumerate(fields.items()):
        names[f"#f{index}"], values[f":v{index}"] = key, value
        updates.append(f"#f{index}=:v{index}")
    registry.update_item(
        Key={"pk": "BATCH#" + batch_id},
        UpdateExpression="SET " + ",".join(updates),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def notify(batch, reason):
    """Send a concise operational alert without customer payloads."""
    sns.publish(
        TopicArn=os.environ["ALERT_TOPIC"],
        Subject="Meesho delivery requires attention",
        Message=json.dumps({"batch_id": batch, "reason": reason}),
    )


def reconcile(batch):
    """Read crawler/job status directly and resolve interrupted workflow starts."""
    run_id = batch.get("workflow_run_id")
    if not run_id:
        # A timeout may occur after AWS accepted StartWorkflowRun. Do not blindly
        # start a second workflow. The attempt marker disambiguates old retries.
        token = None
        while True:
            params = {"Name": WORKFLOW, "MaxResults": 100}
            if token:
                params["NextToken"] = token
            page = glue.get_workflow_runs(**params)
            for candidate in page["Runs"]:
                props = candidate.get("WorkflowRunProperties", {})
                if props.get("batch_id") == batch["batch_id"] and props.get(
                    "attempt"
                ) == str(batch["attempt"]):
                    run_id = candidate["WorkflowRunId"]
                    set_state(batch["batch_id"], "RUNNING", workflow_run_id=run_id)
                    break
            if run_id or not page.get("NextToken"):
                break
            token = page["NextToken"]
        if not run_id:
            age = (
                datetime.now(timezone.utc) - datetime.fromisoformat(batch["started_at"])
            ).total_seconds()
            if age > 600 and batch["state"] != "NEEDS_REVIEW":
                set_state(
                    batch["batch_id"],
                    "NEEDS_REVIEW",
                    error="Start result uncertain; inspect Glue runs.",
                )
                notify(
                    batch["batch_id"],
                    "Workflow start uncertain. Registry claim retained.",
                )
            return
    run = glue.get_workflow_run(Name=WORKFLOW, RunId=run_id, IncludeGraph=True)["Run"]
    # The workflow graph includes actual job and crawler executions. Keep their
    # latest states on the batch item; no event rule, extra Lambda or S3 audit is
    # needed. Missing nodes remain NOT_STARTED instead of implying success.
    steps = {}
    for node in run.get("Graph", {}).get("Nodes", []):
        if node.get("Type") not in {"JOB", "CRAWLER"}:
            continue
        is_job = node["Type"] == "JOB"
        executions = node.get("JobDetails" if is_job else "CrawlerDetails", {}).get(
            "JobRuns" if is_job else "Crawls", []
        )
        latest = max(
            executions, key=lambda item: str(item.get("StartedOn", "")), default={}
        )
        state = latest.get("JobRunState" if is_job else "State", "NOT_STARTED")
        steps[node["Name"]] = {
            "type": node["Type"],
            "state": state,
            "run_id": latest.get("Id", ""),
            "started_at": str(latest.get("StartedOn", "")),
            "completed_at": str(latest.get("CompletedOn", "")),
            "error": latest.get("ErrorMessage", "")[:2000],
        }
    registry.update_item(
        Key={"pk": batch["pk"]},
        UpdateExpression="SET workflow_status=:status, execution_steps=:steps, checked_at=:time",
        ExpressionAttributeValues={
            ":status": run["Status"],
            ":steps": steps,
            ":time": now(),
        },
    )
    failed = [
        name for name, step in steps.items() if step["state"] in TERMINAL_FAILURES
    ]
    current = registry.get_item(Key={"pk": batch["pk"]}, ConsistentRead=True)["Item"]
    # Publication alone is insufficient: Gold could fail after its Iceberg commit.
    # All four actions (crawler + three jobs) must finish before releasing CONTROL.
    complete = len(steps) == 4 and all(
        step["state"] == "SUCCEEDED" for step in steps.values()
    )
    if (
        current["state"] == "SUCCEEDED"
        and current.get("publication_id") == run_id
        and run["Status"] == "COMPLETED"
        and complete
    ):
        if current.get("quality_status") == "WARNING" and not current.get(
            "warning_notified"
        ):
            notify(
                batch["batch_id"],
                "Published with quality warnings; inspect quality/ and quarantine/.",
            )
            registry.update_item(
                Key={"pk": batch["pk"]},
                UpdateExpression="SET warning_notified=:yes",
                ExpressionAttributeValues={":yes": True},
            )
        registry.delete_item(
            Key={"pk": "CONTROL"},
            ConditionExpression="batch_id=:b",
            ExpressionAttributeValues={":b": batch["batch_id"]},
        )
    elif (failed or run["Status"] in {"STOPPED", "ERROR", "COMPLETED"}) and current[
        "state"
    ] not in {"FAILED", "NEEDS_REVIEW"}:
        reason = (
            "Failed workflow steps: " + ",".join(failed)
            if failed
            else "Workflow ended without all four successful steps and publication confirmation."
        )
        set_state(batch["batch_id"], "FAILED", error=reason)
        notify(batch["batch_id"], reason)


def handler(event, context):
    # CodeBuild uses this gate to block new admissions before replacing runtime
    # code. Existing CONTROL ownership can still be reconciled while paused.
    if event.get("action") == "pause":
        registry.put_item(Item={"pk": "MAINTENANCE"})
        return {"state": "PAUSED"}
    if event.get("action") == "resume":
        registry.delete_item(Key={"pk": "MAINTENANCE"})
        return {"state": "RESUMED"}
    control = registry.get_item(Key={"pk": "CONTROL"}, ConsistentRead=True).get("Item")
    if event.get("action") in {"retry", "reject", "recover"}:
        batch_id = event["batch_id"]
        batch = registry.get_item(
            Key={"pk": "BATCH#" + batch_id}, ConsistentRead=True
        ).get("Item")
        if not batch or batch["state"] not in {"FAILED", "SUCCEEDED"}:
            raise ValueError(
                "Retry requires a FAILED or SUCCEEDED batch; uncertain starts require investigation."
            )
        if control and control["batch_id"] != batch_id:
            raise ValueError(
                "Another delivery owns the workflow. Retry after it completes."
            )
        if batch.get("workflow_run_id"):
            status = glue.get_workflow_run(
                Name=WORKFLOW, RunId=batch["workflow_run_id"]
            )["Run"]["Status"]
            if status not in {"COMPLETED", "STOPPED", "ERROR"}:
                raise ValueError("The previous workflow is still active.")
        if event["action"] == "reject":
            if (
                batch["state"] != "FAILED"
                or not event.get("reason")
                or not batch.get("workflow_run_id")
            ):
                raise ValueError("Reject requires a failed run and an operator reason.")
            if batch.get("silver_write_started"):
                raise ValueError(
                    "Silver writes were attempted. Retry or recover with a complete replacement delivery."
                )
            set_state(batch_id, "REJECTED", error=event["reason"])
        elif event["action"] == "recover":
            if batch["state"] != "FAILED" or not event.get("reason"):
                raise ValueError("Recovery requires a failed batch and a reason.")
            replacement_id = event["replacement_batch_id"]
            replacement = registry.get_item(
                Key={"pk": "BATCH#" + replacement_id}, ConsistentRead=True
            ).get("Item")
            if not replacement or replacement["state"] != "PENDING":
                raise ValueError("Replacement must be a registered PENDING delivery.")
            s3 = boto3.client("s3")

            def file_set(item):
                manifest = json.loads(
                    s3.get_object(
                        Bucket=os.environ["DATA_BUCKET"],
                        Key=item["manifest_key"],
                        VersionId=item["manifest_version"],
                    )["Body"].read()
                )
                return {(f["key"], f["version_id"]) for f in manifest["files"]}

            if not file_set(batch).issubset(file_set(replacement)):
                raise ValueError(
                    "Recovery must include every original file plus corrected or missing source events."
                )
            set_state(
                replacement_id,
                "PENDING",
                recovered_from=batch_id,
                recovery_reason=event["reason"],
            )
            registry.put_item(Item={"pk": "CONTROL", "batch_id": replacement_id})
            set_state(batch_id, "SUPERSEDED", replacement_batch_id=replacement_id)
            return start(replacement, claim=False)
        else:
            set_state(batch_id, "PENDING", workflow_run_id="", error="")
        if control:
            registry.delete_item(Key={"pk": "CONTROL"})
        return {
            "batch_id": batch_id,
            "state": "REJECTED" if event["action"] == "reject" else "PENDING",
        }
    if control:
        batch = registry.get_item(
            Key={"pk": "BATCH#" + control["batch_id"]}, ConsistentRead=True
        )["Item"]
        if batch["state"] != "PENDING":
            reconcile(batch)
            return {"batch_id": batch["batch_id"]}
        # A prior invocation may have stopped after taking CONTROL but before
        # persisting STARTING. No external call was made in this state.
        return start(batch, claim=False)
    if registry.get_item(Key={"pk": "MAINTENANCE"}, ConsistentRead=True).get("Item"):
        return {"state": "PAUSED"}
    pending = registry.query(
        IndexName="state-created",
        KeyConditionExpression=Key("state").eq("PENDING"),
        Limit=1,
    )["Items"]
    if not pending:
        return {"state": "IDLE"}
    batch = registry.get_item(Key={"pk": pending[0]["pk"]}, ConsistentRead=True)["Item"]
    if batch["state"] != "PENDING":
        return {"state": "INDEX_CATCHING_UP"}
    return start(batch)


def start(batch, claim=True):
    """Persist the attempt before the non-idempotent Glue start API call."""
    batch_id = batch["batch_id"]
    if claim:
        registry.put_item(
            Item={"pk": "CONTROL", "batch_id": batch_id},
            ConditionExpression="attribute_not_exists(pk)",
        )
    attempt = int(batch["attempt"]) + 1
    set_state(
        batch_id,
        "STARTING",
        attempt=attempt,
        started_at=now(),
        workflow_run_id="",
        release_id=os.environ["RELEASE_ID"],
        quality_status="PASSED",
        quality_warning_count=0,
        warning_notified=False,
        execution_steps={},
        workflow_status="STARTING",
    )
    # The durable STARTING state precedes the external call. Reconcile handles an
    # ambiguous response; the claim is deliberately not released in an exception.
    result = glue.start_workflow_run(
        Name=WORKFLOW,
        RunProperties={
            "batch_id": batch_id,
            "manifest_key": batch["manifest_key"],
            "manifest_version": batch["manifest_version"],
            "attempt": str(attempt),
        },
    )
    set_state(batch_id, "RUNNING", workflow_run_id=result["RunId"])
    return {"batch_id": batch_id, "workflow_run_id": result["RunId"]}
