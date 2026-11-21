"""Coordinate the deployment commands run by the deployment CodeBuild project.

Who uses this file:
    buildspec.yml runs `python ops/deploy.py` after validation succeeds.
    Bootstrap supplies AWS resource references; CodePipeline identifies the branch
    and commit; config/dev.yaml or config/prod.yaml supplies environment settings.

Flow: validate target -> pause/drain existing processing -> deploy foundation ->
publish application files -> deploy platform -> check workflow -> resume admissions.
CloudFormation creates/updates resources and IAM permissions. This script orders
those operations; it does not upload business data or request a new delivery run.
If deployment fails after pausing, processing stays paused for investigation.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import time

import boto3
from botocore.exceptions import ClientError
from control import invoke, schedule
from environment import check_pipeline, load_config
from package import publish

# The parent of this file is ops/; parents[1] is the cloned repository directory.
# Template paths therefore work even if CodeBuild starts the command elsewhere.
ROOT = Path(__file__).resolve().parents[1]


def describe_stack(client, name):
    """Return one stack's details, or None when AWS says that stack does not exist."""
    try:
        # describe_stacks returns a list. We requested one stack by name, so [0]
        # selects that stack's complete record, including status and Outputs.
        return client.describe_stacks(StackName=name)["Stacks"][0]
    except ClientError as error:
        # AWS also uses ValidationError for other invalid requests. Check the message
        # as well so a genuine API/permission problem cannot look like a first deployment.
        if (
            error.response["Error"]["Code"] == "ValidationError"
            and "does not exist" in error.response["Error"]["Message"]
        ):
            return None
        # Keep all other failures visible to CodeBuild and stop the deployment.
        raise


def report_stack_failure(stack_name, started_at):
    """Copy this deployment's CloudFormation failures into the CodeBuild log.

    The CLI's exit code does not explain which resource failed. Events supply the
    logical resource name and AWS's reason, including errors during rollback.
    This diagnostic must never replace the original deployment exception.
    """
    print(f"CloudFormation failures for {stack_name}:", flush=True)
    try:
        client = boto3.client("cloudformation")
        failures = []
        # AWS returns newest events first and can split them across several pages.
        pages = client.get_paginator("describe_stack_events").paginate(
            StackName=stack_name
        )
        for page in pages:
            events = page["StackEvents"]
            failures.extend(
                event
                for event in events
                if event["Timestamp"] >= started_at
                and event["ResourceStatus"].endswith("_FAILED")
            )
            # Older events belong to a previous attempt; do not report them as new failures.
            if any(event["Timestamp"] < started_at for event in events):
                break
        # Show the initial failure before the rollback errors it may have caused.
        for event in sorted(failures, key=lambda item: item["Timestamp"]):
            print(
                f"{event['LogicalResourceId']} | {event['ResourceStatus']} | "
                f"{event.get('ResourceStatusReason', 'No reason supplied by AWS')}",
                flush=True,
            )
        if not failures:
            print("No new failed events; check the AWS CLI output above.", flush=True)
    except Exception as error:
        # For example, an API access error must not hide the deployment's exit code.
        print(f"Could not read stack events: {error}", flush=True)


def deploy_stack(project, name, role, parameters):
    """Create/update foundation or platform and wait for the AWS CLI to finish.

    project selects the environment; name selects the template (foundation/platform).
    role is CloudFormation's execution-role ARN. parameters supplies template inputs.
    These CLI commands run inside CodeBuild, not in the developer's terminal.
    """
    # Pass arguments as a list: subprocess starts the AWS CLI without a shell parsing
    # the values. The CLI prepares a change set, applies it and waits for completion.
    stack_name = f"{project}-{name}"
    # Record the attempt's start so diagnostics exclude older deployment failures.
    started_at = datetime.now(timezone.utc)
    command = [
        "aws",
        "cloudformation",
        "deploy",
        "--stack-name",
        stack_name,
        "--template-file",
        str(ROOT / "cloudformation" / f"{name}.yaml"),
        # CloudFormation assumes this role to create/update the stack's resources.
        "--role-arn",
        role,
        # Acknowledge that the template can manage IAM roles with explicit names.
        "--capabilities",
        "CAPABILITY_NAMED_IAM",
        # An unchanged template is a successful no-op, not a failed deployment.
        "--no-fail-on-empty-changeset",
        "--parameter-overrides",
        # Expand the Python dictionary into CLI arguments such as Project=meesho-dev.
        *[f"{key}={value}" for key, value in parameters.items()],
    ]
    try:
        # A nonzero CLI exit raises an exception; later deployment steps must not run.
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError:
        report_stack_failure(stack_name, started_at)
        raise


def drain(project, registry):
    """Block new admissions and finish an existing run before replacing its code.

    The coordinator's reserved concurrency serializes the pause action with any
    in-flight admission. After disabling its schedule, this loop invokes the
    coordinator directly to refresh the existing run's state. The MAINTENANCE
    item prevents those invocations from admitting another pending delivery.
    """
    # 1. Close the admission gate first, then disable regular scheduler invocations.
    # Pausing does not cancel a Glue workflow that has already started.
    invoke(project, {"action": "pause"})
    schedule(project, "DISABLED")

    # 2. Allow up to 5,400 seconds (90 minutes) for the delivery to release ownership.
    # monotonic measures elapsed time without being affected by clock adjustments.
    deadline = time.monotonic() + 5400
    while time.monotonic() < deadline:
        # CONTROL identifies the delivery holding the workflow slot. A strongly
        # consistent read asks DynamoDB for the latest committed ownership record.
        owner = registry.get_item(Key={"pk": "CONTROL"}, ConsistentRead=True).get(
            "Item"
        )
        if not owner:
            # No tracked delivery owns the workflow, so deployment may proceed.
            return

        # 3. Ask the coordinator to check Glue and update the batch before reading it.
        # {} selects the coordinator's normal status/dispatch path; maintenance blocks dispatch.
        invoke(project, {})
        batch = registry.get_item(
            Key={"pk": "BATCH#" + owner["batch_id"]}, ConsistentRead=True
        )["Item"]
        # These states require recovery or investigation. Continuing could replace
        # code while an unresolved delivery still owns partially written results.
        if batch["state"] in {"FAILED", "NEEDS_REVIEW"}:
            raise RuntimeError(
                "Resolve delivery "
                + owner["batch_id"]
                + " before redeploying; dispatch remains paused."
            )
        print(f"Waiting for delivery {owner['batch_id']}: {batch['state']}", flush=True)
        # Flush prints immediately to build logs; poll at 15-second intervals.
        time.sleep(15)

    # 4. Exiting on timeout leaves maintenance in place; no automatic resume on failure.
    raise TimeoutError(
        "Active delivery did not drain in 90 minutes; dispatch remains paused."
    )


def verify_workflow(client, project):
    """Check the deployed workflow's crawler/job membership and concurrency setting.

    This reads its definition only; it does not execute Glue jobs or prove that
    data processing succeeds. The first real delivery verifies that separately.
    """
    # IncludeGraph requests the workflow's nodes, including crawlers, jobs and triggers.
    workflow = client.get_workflow(Name=project + "-workflow", IncludeGraph=True)[
        "Workflow"
    ]
    # Keep just crawler/job nodes as (type, name) pairs. A set ignores display order.
    # Trigger nodes are excluded because this check compares processing components.
    actions = {
        (node["Type"], node["Name"])
        for node in workflow["Graph"]["Nodes"]
        if node["Type"] in {"CRAWLER", "JOB"}
    }
    # Require exactly one crawler and the three named layer jobs for this environment.
    expected = {("CRAWLER", project + "-raw-discovery")}
    expected.update(
        ("JOB", project + "-" + stage) for stage in ["bronze", "silver", "gold"]
    )
    # One concurrent workflow prevents different deliveries from writing together.
    # Raise before enabling admissions if the deployed definition is unexpected.
    if actions != expected or workflow["MaxConcurrentRuns"] != 1:
        raise RuntimeError(
            "Deployed workflow is missing its crawler or jobs, or concurrency is incorrect."
        )


def deploy(variables):
    """Run a complete release using CodeBuild's environment-variable dictionary."""
    # Before deployment: confirm the branch/environment/names before any AWS call.
    config = load_config(variables["TARGET_ENVIRONMENT"])
    check_pipeline(config, variables)
    project = config["project"]
    # Combine environment, Git commit and build number to distinguish this release,
    # including another build of the same commit. S3 files are stored under this ID.
    release = f"{config['environment']}-{variables['CODEBUILD_RESOLVED_SOURCE_VERSION']}-{variables['CODEBUILD_BUILD_NUMBER']}"

    # Bootstrap provides the execution role and encryption key; the developer does
    # not need to copy those values into the application configuration or export them.
    role, key = variables["DEPLOYMENT_ROLE_ARN"], variables["KEY_ARN"]
    cloudformation = boto3.client("cloudformation")

    # 1. An existing platform may be processing a delivery. Finish that work first.
    # A new environment has no coordinator/registry yet and skips maintenance.
    previous = describe_stack(cloudformation, project + "-platform")
    if previous:
        # A failed first creation may have deleted the coordinator and registry.
        # Only a stable, successfully created platform can enter maintenance safely.
        status = previous["StackStatus"]
        if status not in {
            "CREATE_COMPLETE",
            "UPDATE_COMPLETE",
            "UPDATE_ROLLBACK_COMPLETE",
        }:
            raise RuntimeError(
                f"{project}-platform is {status}. Recover the stack in the "
                "CloudFormation Console before retrying; see RUNBOOK.md section 12. "
                "No deployment changes were made by this build."
            )
        print("1. Pause new deliveries and drain the active workflow.", flush=True)
        drain(project, boto3.resource("dynamodb").Table(project + "-registry"))
    else:
        print("1. First deployment: no schedule or registry exists yet.", flush=True)

    # 2. Ensure storage exists before the platform receives its data-bucket reference.
    print("2. Create/update the data foundation from Git.", flush=True)
    deploy_stack(project, "foundation", role, {"Project": project, "KeyArn": key})
    foundation = describe_stack(cloudformation, project + "-foundation")
    # CloudFormation returns Outputs as a list of key/value records. Convert it to
    # a dictionary so outputs["DataBucket"] reads the deployed bucket's actual name.
    outputs = {item["OutputKey"]: item["OutputValue"] for item in foundation["Outputs"]}

    # 3. Publish this commit's Lambda ZIP, Glue scripts and view SQL before any
    # resource is pointed at the new release. Upload failure stops the deployment.
    print("3. Package and upload the immutable code release: " + release, flush=True)
    publish(variables["ARTIFACT_BUCKET"], release)
    print(
        "4. Create/update the platform, including runtime IAM permissions.", flush=True
    )
    # 4. Deploy the runtime services/permissions using those bucket and release values.
    # Keep the schedule disabled until the definition check below succeeds.
    deploy_stack(
        project,
        "platform",
        role,
        {
            "Project": project,
            "DatabasePrefix": config["database_prefix"],
            "DataBucket": outputs["DataBucket"],
            "ArtifactBucket": variables["ARTIFACT_BUCKET"],
            "KeyArn": key,
            "ReleaseId": release,
            "ScheduleState": "DISABLED",
            "AlertEmail": config["alert_email"],
        },
    )
    # 5. Confirm the deployed workflow, then restore automatic processing.
    print(
        "5. Verify the crawler/job graph, then enable delivery processing.", flush=True
    )
    verify_workflow(boto3.client("glue"), project)
    # The maintenance gate remains closed through stack deployment and verification.
    # Enable scheduling before removing it; an intervening tick cannot admit work.
    schedule(project, "ENABLED")
    invoke(project, {"action": "resume"})
    # Successful return lets CodeBuild, and then CodePipeline, mark deployment successful.
    print(
        f"READY: {project}; data bucket={outputs['DataBucket']}; release={release}",
        flush=True,
    )


if __name__ == "__main__":
    # CodeBuild runs this entry point. Imported helpers remain available to offline tests.
    deploy(os.environ)
