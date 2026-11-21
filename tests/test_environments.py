"""Check branch routing and resource separation without creating AWS resources."""

import importlib.util
from fnmatch import fnmatchcase
import json
from pathlib import Path
import re
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "environment", ROOT / "ops/environment.py"
)
environment = importlib.util.module_from_spec(spec)
spec.loader.exec_module(environment)


class TemplateLoader(yaml.SafeLoader):
    """Preserve CloudFormation intrinsic functions so tests can select each branch."""


def intrinsic(loader, tag, node):
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node)
    else:
        value = loader.construct_mapping(node)
    return {tag: value}


TemplateLoader.add_multi_constructor("!", intrinsic)


def template(name):
    return yaml.load(
        (ROOT / "cloudformation" / name).read_text(), Loader=TemplateLoader
    )


def resolve(value, parameters, conditions=None):
    """Evaluate only the Ref/Sub/If/Equals expressions exercised by these assertions."""
    if isinstance(value, list):
        return [resolve(item, parameters, conditions) for item in value]
    if not isinstance(value, dict):
        return value
    if "Ref" in value:
        return parameters[value["Ref"]]
    if "Sub" in value:
        return re.sub(r"\$\{([^}]+)\}", lambda m: parameters[m[1]], value["Sub"])
    if "Equals" in value:
        left, right = resolve(value["Equals"], parameters, conditions)
        return left == right
    if "If" in value:
        condition, yes, no = value["If"]
        chosen = yes if resolve(conditions[condition], parameters) else no
        return resolve(chosen, parameters, conditions)
    return {key: resolve(item, parameters, conditions) for key, item in value.items()}


class EnvironmentTests(unittest.TestCase):
    def test_pipeline_target_rejects_wrong_branch_and_cross_environment_names(self):
        for name in ["dev", "prod"]:
            config = environment.load_config(name)
            variables = {
                "TARGET_ENVIRONMENT": name,
                "SOURCE_BRANCH": config["branch"],
                "PROJECT": config["project"],
                "DATABASE_PREFIX": config["database_prefix"],
            }
            environment.check_pipeline(config, variables)
            for key in variables:
                with self.subTest(environment=name, changed=key):
                    with self.assertRaises(ValueError):
                        environment.check_pipeline(
                            config, {**variables, key: "wrong-target"}
                        )

    def test_config_rejects_production_names_in_development_file(self):
        wrong = {
            "environment": "dev",
            "branch": "dev",
            "project": "meesho-prod",
            "database_prefix": "meesho_prod",
        }
        with patch.object(environment.yaml, "safe_load", return_value=wrong):
            with self.assertRaises(ValueError):
                environment.load_config("dev")

    def test_source_routing_and_production_only_approval(self):
        delivery = template("bootstrap.yaml")
        for name in ["dev", "prod"]:
            config = environment.load_config(name)
            params = {
                "Environment": name,
                "ConnectionArn": "connection",
                "Repository": "owner/repo",
                "Validate": "validate",
                "Deploy": "deploy",
                "AWS::NoValue": None,
            }
            stages = resolve(
                delivery["Resources"]["Pipeline"]["Properties"]["Stages"],
                params,
                delivery["Conditions"],
            )
            stages = [stage for stage in stages if stage is not None]
            expected = (
                ["Source", "Validate", "Deploy"]
                if name == "dev"
                else ["Source", "Validate", "Approve", "Deploy"]
            )
            self.assertEqual([stage["Name"] for stage in stages], expected)
            source = stages[0]["Actions"][0]["Configuration"]
            self.assertEqual(source["BranchName"], "dev" if name == "dev" else "main")
            self.assertEqual(source["DetectChanges"], "true")
        # Branch selection is derived from Environment, not a second user input.
        self.assertEqual(
            set(delivery["Parameters"]), {"Environment", "Repository", "ConnectionArn"}
        )
        self.assertLess((ROOT / "cloudformation/bootstrap.yaml").stat().st_size, 51200)

    def test_physical_resource_names_do_not_overlap_in_one_account(self):
        names = {"dev": set(), "prod": set()}
        name_fields = {
            "Name",
            "BucketName",
            "TableName",
            "FunctionName",
            "RoleName",
            "QueueName",
            "TopicName",
            "AlarmName",
            "LogGroupName",
            "DashboardName",
        }
        for target in names:
            config = environment.load_config(target)
            params = {
                "Environment": target,
                "Project": config["project"],
                "DatabasePrefix": config["database_prefix"],
                "AWS::AccountId": "123456789012",
                "AWS::Region": "us-east-1",
            }
            for filename in [
                "foundation.yaml",
                "platform.yaml",
                "bootstrap.yaml",
            ]:
                for resource in template(filename)["Resources"].values():
                    props = resource["Properties"]
                    for key in name_fields & props.keys():
                        if isinstance(props[key], dict) and "Sub" in props[key]:
                            names[target].add(resolve(props[key], params))
                    if "DatabaseInput" in props:
                        names[target].add(
                            resolve(props["DatabaseInput"]["Name"], params)
                        )
            self.assertIn(config["project"] + "-workflow", names[target])
            self.assertIn(config["database_prefix"] + "_gold", names[target])
        self.assertTrue(names["dev"].isdisjoint(names["prod"]))

    def test_crawler_is_workflow_root_and_audit_path_is_absent(self):
        resources = template("platform.yaml")["Resources"]
        self.assertEqual(
            resources["StartTrigger"]["Properties"]["Actions"],
            [{"CrawlerName": {"Ref": "Crawler"}}],
        )
        condition = resources["BronzeTrigger"]["Properties"]["Predicate"]["Conditions"][
            0
        ]
        self.assertEqual(condition["CrawlerName"], {"Ref": "Crawler"})
        self.assertEqual(condition["CrawlState"], "SUCCEEDED")
        self.assertEqual(
            len(resources["Crawler"]["Properties"]["Targets"]["S3Targets"]), 7
        )
        self.assertEqual(
            sum(r["Type"] == "AWS::Lambda::Function" for r in resources.values()), 2
        )
        self.assertEqual(
            sum(r["Type"] == "AWS::Events::Rule" for r in resources.values()), 1
        )
        for removed in [
            "Audit",
            "JobEvents",
            "AuditTable",
            "OpsDatabase",
            "AuditErrors",
        ]:
            self.assertNotIn(removed, resources)

    def test_validation_uses_a_role_without_deployment_permissions(self):
        resources = template("bootstrap.yaml")["Resources"]
        self.assertEqual(
            resources["Validate"]["Properties"]["ServiceRole"],
            {"GetAtt": "ValidationRole.Arn"},
        )
        self.assertEqual(
            resources["Deploy"]["Properties"]["ServiceRole"],
            {"GetAtt": "BuildRole.Arn"},
        )
        statements = resources["ValidationRole"]["Properties"]["Policies"][0][
            "PolicyDocument"
        ]["Statement"]
        for statement in statements:
            actions = statement["Action"]
            for action in actions if isinstance(actions, list) else [actions]:
                self.assertIn(action.split(":")[0], {"s3", "kms", "logs"})
                self.assertNotIn(
                    action, {"s3:PutObject", "s3:DeleteObject", "s3:*", "kms:*"}
                )

    def test_crawler_table_level_matches_each_entity_target(self):
        crawler = template("platform.yaml")["Resources"]["Crawler"]["Properties"]
        level = json.loads(crawler["Configuration"])["Grouping"][
            "TableLevelConfiguration"
        ]
        for target in crawler["Targets"]["S3Targets"]:
            path = resolve(target["Path"], {"DataBucket": "example-bucket"})
            # Glue counts the bucket as level 1. The table belongs at the entity
            # folder, not above the include path or below it among individual files.
            parts = path.removeprefix("s3://").strip("/").split("/")
            with self.subTest(path=path):
                self.assertEqual(level, len(parts))

    def test_database_deletion_covers_children_without_cross_environment_access(self):
        policies = template("bootstrap.yaml")["Resources"]["DeploymentRole"][
            "Properties"
        ]["Policies"]
        for target, other in [("dev", "prod"), ("prod", "dev")]:
            params = {
                "Environment": target,
                "AWS::Partition": "aws",
                "AWS::AccountId": "123456789012",
                "AWS::Region": "us-east-1",
            }
            allowed = []
            for policy in policies:
                for statement in policy["PolicyDocument"]["Statement"]:
                    actions = statement["Action"]
                    actions = actions if isinstance(actions, list) else [actions]
                    if statement["Effect"] == "Allow" and any(
                        fnmatchcase("glue:DeleteDatabase", action) for action in actions
                    ):
                        resources = resolve(statement["Resource"], params)
                        allowed.extend(
                            resources if isinstance(resources, list) else [resources]
                        )
            prefix = "arn:aws:glue:us-east-1:123456789012:"
            # AWS authorizes database deletion against all four resource types,
            # including the child-function wildcard even when no functions exist.
            self.assertTrue(
                any(fnmatchcase(prefix + "catalog", pattern) for pattern in allowed)
            )
            for layer in ["raw", "bronze", "silver", "gold"]:
                for resource in [
                    f"database/meesho_{target}_{layer}",
                    f"table/meesho_{target}_{layer}/*",
                    f"userDefinedFunction/meesho_{target}_{layer}/*",
                ]:
                    with self.subTest(environment=target, resource=resource):
                        self.assertTrue(
                            any(
                                fnmatchcase(prefix + resource, pattern)
                                for pattern in allowed
                            )
                        )
                        foreign = resource.replace(
                            f"meesho_{target}_", f"meesho_{other}_"
                        )
                        self.assertFalse(
                            any(
                                fnmatchcase(prefix + foreign, pattern)
                                for pattern in allowed
                            )
                        )


if __name__ == "__main__":
    unittest.main()
