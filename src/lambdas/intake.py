"""Register complete S3 delivery manifests. SQS retries failed records only.

Only manifests trigger intake. Data objects are uploaded first under immutable,
content-addressed keys. Registry state survives SQS acknowledgement and retries.
"""

import hashlib
import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
registry = boto3.resource("dynamodb").Table(os.environ["REGISTRY_TABLE"])
BUCKET = os.environ["DATA_BUCKET"]


def validate_manifest(manifest):
    """Reject arbitrary paths, oversized submissions and ambiguous file lists."""
    files = manifest.get("files", [])
    if manifest.get("schema_version") != 1 or not 1 <= len(files) <= 100:
        raise ValueError("Manifest requires schema_version=1 and 1..100 files.")
    keys = set()
    for item in files:
        digest = item.get("sha256", "")
        entity = item.get("entity", "")
        expected = f"landing/objects/{entity}/{digest}.jsonl"
        if (
            entity
            not in {
                "customer",
                "product",
                "order_item",
                "payment",
                "shipment",
                "return",
                "refund",
            }
            or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)
            or item.get("key") != expected
            or not item.get("version_id")
            or not isinstance(item.get("rows"), int)
            or item["rows"] < 1
            or expected in keys
        ):
            raise ValueError("Invalid or repeated manifest file entry.")
        keys.add(expected)
    return files


def register(key, version):
    """The manifest body digest is the logical batch identity across redelivery."""
    if not key.startswith("landing/manifests/"):
        raise ValueError("Only landing/manifests/ objects are accepted.")
    raw = s3.get_object(Bucket=BUCKET, Key=key, VersionId=version)["Body"].read()
    if len(raw) > 1024 * 1024:
        raise ValueError("Manifest exceeds 1 MiB.")
    manifest = json.loads(raw)
    for item in validate_manifest(manifest):
        head = s3.head_object(Bucket=BUCKET, Key=item["key"])
        if (
            head["VersionId"] != item["version_id"]
            or head.get("Metadata", {}).get("sha256") != item["sha256"]
        ):
            raise ValueError(
                "Manifest object version/checksum does not match landing data."
            )
    batch = hashlib.sha256(raw).hexdigest()
    try:
        registry.put_item(
            Item={
                "pk": "BATCH#" + batch,
                "batch_id": batch,
                "state": "PENDING",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "manifest_key": key,
                "manifest_version": version,
                "attempt": 0,
                "mode": manifest.get("mode", "INCREMENTAL"),
            },
            ConditionExpression="attribute_not_exists(pk)",
        )
    except ClientError as error:
        if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    return batch


def handler(event, context):
    failures = []
    for record in event["Records"]:
        try:
            detail = json.loads(record["body"])["detail"]
            if detail["bucket"]["name"] != BUCKET:
                raise ValueError("Unexpected source bucket.")
            register(detail["object"]["key"], detail["object"]["version-id"])
        except Exception as error:
            print(json.dumps({"message_id": record["messageId"], "error": str(error)}))
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
