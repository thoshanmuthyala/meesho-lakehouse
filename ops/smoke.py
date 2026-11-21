"""Run the supplied acceptance delivery against a deployed AWS environment.

Who uses this file:
    An operator chooses to run this integration check after deployment. Normal
    CI/CD does not run it or upload business data automatically.

Flow: upload the committed delivery -> register its manifest -> request a replay
if this batch already finished -> wait for processing -> check Athena counts/totals.
It reuses the other ops helpers instead of implementing a second processing path.

Use an environment whose business data matches this delivery: expected.json
contains fixed acceptance totals. This command calls real AWS services, can start
Glue processing, and does not delete existing rows to obtain the expected totals.
"""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import boto3
from control import invoke

# Find the repository above ops/ so subprocesses and input files use absolute paths.
ROOT = Path(__file__).resolve().parents[1]


def main():
    """Execute the acceptance sequence and stop if any helper or verification fails."""
    # 1. Read the existing environment's resource names. No stack is created here.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--database-prefix", required=True)
    args = parser.parse_args()
    # 2. Call the uploader with the supplied delivery's manifest.
    # sys.executable uses this same Python interpreter and installed dependencies.
    # check_output waits for completion, raises on failure, and captures printed JSON;
    # text=True returns a string instead of raw bytes for json.loads below.
    output = subprocess.check_output(
        [
            sys.executable,
            str(ROOT / "ops/upload_delivery.py"),
            "--bucket",
            args.bucket,
            "--manifest",
            str(ROOT / "data/delivery/manifest.json"),
        ],
        text=True,
    )
    # The uploader derives batch_id from the ready manifest's bytes. Use that exact ID
    # to inspect/replay/wait for this delivery rather than inventing another identifier.
    batch = json.loads(output)["batch_id"]

    # 3. Re-submit notifications for ready manifests already in landing, including this
    # one. Existing registrations are recognized; a lost original S3 notification can
    # therefore be repaired before the registry read below. No files are transformed here.
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "ops/control.py"),
            "--project",
            args.project,
            "reconcile-landing",
            "--bucket",
            args.bucket,
        ],
        # A failed helper raises immediately, so later acceptance steps cannot continue.
        check=True,
    )

    # 4. Read the latest registered state. A strongly consistent read sees the update
    # from the synchronous intake invocation performed by reconcile-landing.
    table = boto3.resource("dynamodb").Table(args.project + "-registry")
    state = table.get_item(Key={"pk": "BATCH#" + batch}, ConsistentRead=True)["Item"][
        "state"
    ]
    if state in {"SUCCEEDED", "FAILED"}:
        # An identical upload reuses its manifest/batch ID. If that batch already
        # succeeded or failed, explicitly request another processing attempt for this check.
        # First let the coordinator finish any outstanding ownership/status reconciliation.
        invoke(args.project, {})
        invoke(args.project, {"action": "retry", "batch_id": batch})

    # 5. The wait helper invokes coordinator ticks and watches this batch until it
    # succeeds and releases ownership. Failed/uncertain/withdrawn outcomes stop this run.
    # Pending or active batches reach this step without being reset to another attempt.
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "ops/control.py"),
            "--project",
            args.project,
            "wait",
            "--batch",
            batch,
        ],
        check=True,
    )
    # 6. Apply the supplied view SQL, then check Bronze/Silver counts and Gold financial
    # totals against data/expected.json. Gold normally prepares these views during its run;
    # this helper also executes the checked-in SQL before running acceptance queries.
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "ops/athena.py"),
            "--project",
            args.project,
            "--database-prefix",
            args.database_prefix,
            "--file",
            str(ROOT / "sql/views.sql"),
            "--verify",
            str(ROOT / "data/expected.json"),
        ],
        check=True,
    )
    # 7. Print success only after upload, processing and every Athena comparison passed.
    print("AWS integration checks passed for batch " + batch)


if __name__ == "__main__":
    # Direct execution performs AWS work; importing this module does not run the check.
    main()
