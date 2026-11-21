"""Check that a deployment uses the intended Git branch and AWS resource names.

Who uses this file:
    Both CodeBuild projects run it before their main work. deploy.py also imports
    its checks, and upload_delivery.py uses load_config() to find the environment.

Flow: read config/dev.yaml or config/prod.yaml -> validate its values -> compare
them with the values supplied by CodePipeline and the bootstrap stack.
This module reads local configuration only; it makes no AWS calls.

CodeBuild usage: python ops/environment.py --environment dev --check-pipeline
"""

import argparse
import os
from pathlib import Path
import re

import yaml

# __file__ is this Python file. Its parent is ops/; parents[1] is the repository.
# Resolve paths from the file location so the caller's current folder does not matter.
ROOT = Path(__file__).resolve().parents[1]


def load_config(environment):
    """Return the selected YAML configuration, or raise ValueError for invalid values.

    This checks the file itself. check_pipeline() separately compares that file
    with the actual pipeline inputs before a build can deploy anything.
    """
    # 1. Accept only the two supported environments before constructing a file path.
    if environment not in {"dev", "prod"}:
        raise ValueError("Environment must be dev or prod.")

    # 2. Turn YAML key/value settings into a Python dictionary.
    # safe_load reads configuration without allowing YAML to construct Python objects.
    with (ROOT / "config" / f"{environment}.yaml").open() as source:
        config = yaml.safe_load(source)

    # Require exactly these keys: missing settings and misspelled extra keys both fail.
    if not isinstance(config, dict) or set(config) != {
        "environment",
        "branch",
        "project",
        "database_prefix",
        "alert_email",
    }:
        raise ValueError(
            "Configuration requires environment, branch, project, database_prefix and alert_email."
        )
    # 3. Development belongs to dev; production belongs to main.
    # Check both fields so a prod file cannot accidentally claim to be a dev file.
    expected_branch = "dev" if environment == "dev" else "main"
    if config["environment"] != environment or config["branch"] != expected_branch:
        raise ValueError(f"{environment} must use branch {expected_branch}.")
    # 4. AWS resource names use hyphens; Glue database names use underscores.
    # fullmatch requires the WHOLE name to follow the pattern, including its suffix.
    # The pipeline comparison below also requires agreement with bootstrap's names.
    for key, pattern in {
        "project": rf"[a-z][a-z0-9-]*-{environment}",
        "database_prefix": rf"[a-z][a-z0-9_]*_{environment}",
    }.items():
        value = config[key]
        if (
            not isinstance(value, str)
            or len(value) > 26
            or not re.fullmatch(pattern, value)
        ):
            raise ValueError(
                f"{key} must use the {environment} suffix and at most 26 characters."
            )
    # 5. A blank email skips the SNS email subscription. Otherwise require email syntax.
    # This checks the text, not mailbox ownership; SNS confirmation happens separately.
    email = config["alert_email"]
    if not isinstance(email, str) or (
        email and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email)
    ):
        raise ValueError("alert_email must be blank or an email address.")
    # Return the checked dictionary for the uploader/deployment code to use.
    return config


def check_pipeline(config, variables):
    """Reject a build whose AWS-supplied target differs from the configuration.

    config is the dictionary returned by load_config(). variables is normally
    os.environ: the environment variables available inside the CodeBuild process.
    """
    # Bootstrap supplies the environment and resource prefixes. CodePipeline supplies
    # SOURCE_BRANCH from its source action, identifying the branch actually fetched.
    expected = {
        "TARGET_ENVIRONMENT": config["environment"],
        "SOURCE_BRANCH": config["branch"],
        "PROJECT": config["project"],
        "DATABASE_PREFIX": config["database_prefix"],
    }
    # Compare each expected value. A missing variable also fails because get() is None.
    # Success simply returns to the caller; any mismatch stops the build with an error.
    for name, value in expected.items():
        if variables.get(name) != value:
            raise ValueError(f"Pipeline {name} must be {value!r}; target check failed.")


def main():
    """Check the configuration supplied to CodeBuild before deployment begins."""
    # 1. Read the arguments used in the buildspec; argparse reports invalid inputs.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True, choices=["dev", "prod"])
    parser.add_argument("--check-pipeline", action="store_true", required=True)
    args = parser.parse_args()
    # 2. Validate the selected file, then compare it with this build's environment.
    config = load_config(args.environment)
    check_pipeline(config, os.environ)

    # 3. Make the confirmed branch-to-project pairing visible in the CodeBuild logs.
    print(f"Confirmed {config['branch']} -> {config['project']}")


if __name__ == "__main__":
    # Run argument parsing only when executed directly, not when another file imports us.
    main()
