"""Prepare the application files that Lambda and Glue will run after deployment.

Who uses this file:
    deploy.py calls publish() from inside CodeBuild after deploying foundation.

Flow: validate the release ID -> ZIP Lambda modules -> collect Glue scripts and
Athena view SQL -> upload them under releases/<release-id>/ in the artifact bucket.
Files in an existing release cannot be overwritten. This packages application
code only; it does not generate/upload business data or create AWS resources.

Optional direct usage:
    python ops/package.py --bucket ARTIFACT_BUCKET --release RELEASE_ID
"""

import argparse
import pathlib
import re
import zipfile
import boto3

# Locate the repository above ops/, regardless of where the command was launched.
ROOT = pathlib.Path(__file__).resolve().parents[1]


def publish(bucket, release):
    """Upload one release to the artifact bucket supplied by bootstrap.

    bucket is an existing S3 bucket name. release identifies this environment,
    Git commit and build; deploy.py passes it to CloudFormation as ReleaseId too.
    """
    # 1. Limit the release ID to a single safe folder name; reject slashes and spaces.
    if not re.fullmatch(r"[A-Za-z0-9_-]+", release):
        raise ValueError(
            "Release must contain letters, digits, underscores or hyphens."
        )
    # 2. Create a local packaging directory in the build workspace.
    # exist_ok permits another run to reuse that directory; mode "w" rebuilds the ZIP.
    build = ROOT / ".build"
    build.mkdir(exist_ok=True)
    archive = build / "lambdas.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zipped:
        # Put modules at the ZIP root so handlers such as intake.handler can import them.
        # path.name keeps only the filename, without the local src/lambdas/ directories.
        for path in sorted((ROOT / "src/lambdas").glob("*.py")):
            zipped.write(path, path.name)

    # 3. Collect pairs of (local file, destination path inside this S3 release).
    # Glue reads its Python scripts directly from S3; those files are not in lambdas.zip.
    s3 = boto3.client("s3")
    files = [(archive, "lambdas.zip")]
    files += [(path, "glue/" + path.name) for path in (ROOT / "src/glue").glob("*.py")]
    # Gold receives this SQL file when it creates/refreshes the Athena reporting views.
    files.append((ROOT / "sql/views.sql", "sql/views.sql"))

    # 4. Upload the exact local bytes using the build role's AWS permissions.
    # If an upload fails, let the exception stop deployment before the platform update.
    for path, suffix in files:
        # IfNoneMatch="*" means create only if this S3 key does not already exist.
        # This prevents a later build from silently changing a previously published file.
        s3.put_object(
            Bucket=bucket,
            Key=f"releases/{release}/{suffix}",
            Body=path.read_bytes(),
            IfNoneMatch="*",
        )
    # 5. Record the release location identifier and file count in the build logs.
    print(f"Uploaded release {release}; {len(files)} artifacts.")


if __name__ == "__main__":
    # Direct execution accepts explicit bucket/release arguments. Normal CI calls
    # publish() from deploy.py, so these arguments need no separate developer command.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--release", required=True)
    args = parser.parse_args()
    publish(args.bucket, args.release)
