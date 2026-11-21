"""Submit an existing delivery to S3 so the event-driven pipeline can process it.

Who uses this file:
    An operator runs it from CloudShell/local Python after deployment; smoke.py
    also calls it for the acceptance delivery. This file never generates data.

Flow: select the data bucket -> read the local manifest -> validate/upload each
JSONL file -> optionally include a previous delivery -> upload the ready manifest.
JSONL means one JSON record per line. The local manifest describes those files;
the ready manifest describes their exact S3 keys, versions, hashes and row counts.

The ready manifest is written last because its S3 event starts intake. Re-uploading
identical content reuses existing objects after checking their stored SHA-256 metadata.
Default usage: python ops/upload_delivery.py --environment dev
"""

import argparse
import hashlib
import json
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from environment import load_config


def put_once(s3, bucket, key, body, digest):
    """Create an immutable S3 object, or reuse a matching existing object's version.

    body contains the bytes to store; digest is their SHA-256 text. The returned
    VersionId lets downstream readers request that exact version from the bucket.
    """
    try:
        # 1. Write only when this object key does not exist. The bucket is versioned,
        # but IfNoneMatch prevents a repeated upload from creating another version.
        # Store the checksum in object metadata so a repeated upload can compare it.
        result = s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            IfNoneMatch="*",
            Metadata={"sha256": digest},
            ContentType="application/json",
        )
        # Return the ID of the newly stored version for the ready-manifest entry.
        return result["VersionId"]
    except ClientError as error:
        # 2. HTTP 412 / PreconditionFailed means the key already exists. Other errors
        # (such as access denied) must stop this upload rather than be treated as reuse.
        if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
            raise

        # 3. HeadObject reads metadata without downloading the existing body.
        # Reuse requires the same stored checksum; missing/different metadata is an error.
        existing = s3.head_object(Bucket=bucket, Key=key)
        if existing.get("Metadata", {}).get("sha256") != digest:
            raise ValueError("Existing object has different content metadata: " + key)
        # Keep the existing version ID so repeated delivery manifests remain consistent.
        return existing["VersionId"]


def main():
    """Validate and upload delivery files, then print the ready manifest's identity."""
    # 1. Choose either automatic environment lookup or an explicit bucket, never both.
    # argparse enforces that choice before any AWS request is made.
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--environment",
        choices=["dev", "prod"],
        help="Resolve the deployed data bucket automatically",
    )
    target.add_argument(
        "--bucket", help="Explicit bucket override for operational tooling"
    )
    # The normal delivery path is relative to this repository, not the current shell folder.
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data/delivery/manifest.json",
    )
    # CloudShell can use its existing identity/Region; a local caller can select a profile.
    parser.add_argument(
        "--profile",
        help="Optional local AWS profile; CloudShell uses its signed-in identity",
    )
    parser.add_argument(
        "--region",
        help="Optional override; otherwise use the configured/CloudShell Region",
    )
    # Recovery can combine new evidence with every file from an earlier ready manifest.
    parser.add_argument(
        "--include-remote-manifest",
        help="Original manifest key for a recovery delivery",
    )
    args = parser.parse_args()
    # 2. Read the local manifest. Keep its delivery-level settings, but rebuild the
    # files list because local filesystem paths must become S3 object references.
    local = json.loads(args.manifest.read_text())
    remote = {k: v for k, v in local.items() if k != "files"}
    remote["files"] = []
    # 3. Create one AWS session so bucket discovery and uploads use the same identity/Region.
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    bucket = args.bucket
    if args.environment:
        # config/dev.yaml or config/prod.yaml gives the stack-name prefix.
        # The foundation stack's Outputs contain the actual bucket name created by AWS.
        project = load_config(args.environment)["project"]
        stack = session.client("cloudformation").describe_stacks(
            StackName=project + "-foundation"
        )["Stacks"][0]
        # next selects the output named DataBucket from CloudFormation's output list.
        # A missing output fails here; do not guess a bucket and risk a wrong destination.
        bucket = next(
            item["OutputValue"]
            for item in stack["Outputs"]
            if item["OutputKey"] == "DataBucket"
        )
    # 4. Process each file listed in the local manifest before publishing readiness.
    s3 = session.client("s3")
    for item in local["files"]:
        # Resolve paths relative to the manifest's directory. Reject ../ or symlink
        # paths that would read a file outside this delivery's directory tree.
        path = (args.manifest.parent / item["path"]).resolve()
        if args.manifest.parent.resolve() not in path.parents:
            raise ValueError("File path must remain inside the delivery directory.")
        # Read the original bytes without rewriting records. Check both the SHA-256
        # content fingerprint and the number of JSONL lines against the local manifest.
        body = path.read_bytes()
        digest = hashlib.sha256(body).hexdigest()
        if digest != item["sha256"] or len(body.splitlines()) != item["rows"]:
            raise ValueError("Local checksum or row count mismatch: " + str(path))
        # The crawler has one input folder per entity. Restrict folders to these seven
        # contracts, then require every JSONL record to declare that same entity type.
        entity = item["entity"]
        if entity not in {
            "customer",
            "product",
            "order_item",
            "payment",
            "shipment",
            "return",
            "refund",
        }:
            raise ValueError("Unknown entity in manifest: " + entity)
        if any(
            json.loads(line).get("entity_type") != entity for line in body.splitlines()
        ):
            raise ValueError("Each source file must contain only its declared entity.")
        # Use the content hash as the filename: unchanged bytes map to the same key.
        # The entity folder lets the crawler discover a separate schema for each entity.
        key = f"landing/objects/{entity}/{digest}.jsonl"
        version = put_once(s3, bucket, key, body, digest)

        # Record exact S3 identity and expected content checks for intake/Glue to consume.
        # No ready manifest is uploaded until every entry has completed this step.
        remote["files"].append(
            {
                "entity": entity,
                "key": key,
                "version_id": version,
                "sha256": digest,
                "rows": item["rows"],
            }
        )
    if args.include_remote_manifest:
        # 5. Optional recovery: read an earlier ready manifest from this same bucket.
        # Keep its original files alongside the correction files uploaded above.
        if not args.include_remote_manifest.startswith("landing/manifests/"):
            raise ValueError("Recovery input must be a landing manifest.")
        previous = json.loads(
            s3.get_object(Bucket=bucket, Key=args.include_remote_manifest)[
                "Body"
            ].read()
        )
        # Index by S3 key to include a repeated file once. If a key is present in both,
        # the new entry wins; the uploader has checked its checksum metadata on reuse.
        combined = {item["key"]: item for item in previous["files"] + remote["files"]}

        # Sort recovery entries by key for stable ordering, and identify the recovery mode.
        remote["files"] = sorted(combined.values(), key=lambda item: item["key"])
        remote["mode"] = "RECOVERY"
    # 6. Serialize the ready manifest consistently: sorted dictionary keys, compact JSON
    # and one trailing newline. Identical content/file order produces identical bytes.
    body = (json.dumps(remote, sort_keys=True, separators=(",", ":")) + "\n").encode()

    # The SHA-256 of those manifest bytes is the batch ID, making repeated identical
    # submissions recognizable as the same delivery to intake and DynamoDB.
    batch = hashlib.sha256(body).hexdigest()
    key = f"landing/manifests/{batch}.json"

    # 7. Publish readiness LAST. Creating this object emits the event that routes through
    # EventBridge/SQS to intake. Reusing an existing manifest does not create a new object.
    version = put_once(s3, bucket, key, body, batch)

    # Print machine-readable JSON for smoke.py and for locating this batch in AWS.
    # Upload completion means files are ready; downstream processing completes separately.
    print(
        json.dumps(
            {"batch_id": batch, "manifest_key": key, "manifest_version": version},
            indent=2,
        )
    )


if __name__ == "__main__":
    # Only direct execution uploads data; importing put_once() for tests has no side effects.
    main()
