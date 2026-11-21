# Meesho: GitHub setup, automatic deployment and data delivery

The normal workflow is **push code → AWS deploys the environment → upload data → AWS processes it**. You do not create individual roles, buckets, Lambda functions or Glue jobs from the terminal.

There are two one-time AWS setup actions: authorize GitHub and create the CI/CD bootstrap stack. A pipeline needs its own artifact storage and permissions before it can read a commit or deploy anything. The bootstrap template creates those prerequisites together. After that, CodePipeline and CodeBuild deploy the project from Git.

| Action | Where you do it | What happens automatically |
|---|---|---|
| Create the repository and its branches | GitHub and your local Git client | Code is versioned in `dev` and `main` |
| Authorize GitHub access once | AWS Connections console | CodePipeline can retrieve the selected repository |
| Upload one bootstrap template per environment | AWS CloudFormation console | CI/CD storage, encryption, roles, CodeBuild projects and CodePipeline are created |
| Push to `dev` | Git client | Validate and deploy `meesho-dev` |
| Merge reviewed changes into `main` | GitHub | Validate, request production approval, then deploy `meesho-prod` |
| Upload a delivery | One upload command in AWS CloudShell | Register the manifest, run Crawler → Bronze → Silver → Gold, and prepare Athena views |

Follow the steps in order for development. Add production after the development deployment and first data delivery succeed. Use **US East (N. Virginia) (`us-east-1`)** throughout this walkthrough; the connection, bootstrap and application must be in the same Region.

## 1. Prepare the local project

Keep the contents of `meesho-lakehouse` at the Git repository root. When GitHub opens the repository, `README.md`, `buildspec.yml`, `cloudformation/`, `src/` and `data/` must be visible immediately.

Your computer needs a Git client and an editor. GitHub Desktop can handle GitHub sign-in, commits and pushes if you prefer its interface. Python 3.11 is needed only if you also want to run validation or the uploader locally. The main deployment procedure does not require local AWS CLI installation or local AWS credentials: CodeBuild runs deployment commands in AWS, and CloudShell provides an authenticated place to upload data.

The repository already contains the source data. Keep `data/` in Git. The `.venv/`, `.build/` and `.local/` directories are ignored and should remain local.

## 2. Create the GitHub repository and branches

1. In GitHub, select **New repository**.
2. Name it **meesho-lakehouse** and choose the required visibility.
3. Create an empty repository. Leave GitHub's README, license and `.gitignore` initialization options unchecked because this folder already contains project files.
4. Copy the repository's HTTPS URL.
5. Open a terminal in your local `meesho-lakehouse` folder. For a folder that is not already a Git repository, run the following, replacing `YOUR_GITHUB_OWNER` with your username or organization:

```bash
git init -b main
git add .
git commit -m "Add Meesho lakehouse"
git remote add origin https://github.com/YOUR_GITHUB_OWNER/meesho-lakehouse.git
git push -u origin main
git switch -c dev
git push -u origin dev
```

These are **local Git commands**, not AWS setup commands. Use your normal GitHub authentication. If Git asks for a commit identity, configure your Git name and email using your Git client. If this folder is already a repository, keep its history and remote; create/push only the missing branches.

6. Refresh GitHub and verify that both `main` and `dev` contain the project files at the root.
7. In the repository's branch protection/ruleset settings, protect `main` with a pull-request review requirement and restrict direct pushes. CodePipeline detects pushes to `main`; GitHub protection ensures those pushes represent reviewed merges.

For subsequent development, keep your working branch on `dev` or use feature branches merged into `dev`.

## 3. Review the environment configuration

The two files below are already supplied. CodeBuild reads the appropriate file automatically.

| Setting | `config/dev.yaml` | `config/prod.yaml` |
|---|---|---|
| Environment | `dev` | `prod` |
| Git branch | `dev` | `main` |
| Resource prefix | `meesho-dev` | `meesho-prod` |
| Glue database prefix | `meesho_dev` | `meesho_prod` |
| Alert email | Blank initially | Blank initially |

Keep the supplied branch and naming values for this setup. The bootstrap derives the same names from its environment selection, and CI rejects mismatches before deploying.

To receive operational emails, set `alert_email` to the desired mailbox in the relevant configuration file, commit and push the change. CloudFormation will create the SNS subscription. The recipient must confirm the AWS subscription email once. Leaving it blank still creates the SNS topic and alarms; it simply omits an email subscription.

No account IDs, bucket names, encryption-key ARNs or execution-role ARNs need to be entered in these files. AWS creates or resolves those values internally.

## 4. Authorize the GitHub connection once

Use an AWS sign-in role authorized to create the project's CloudFormation and IAM resources. The templates create service permissions; they do not change the permissions of your own sign-in identity.

1. Sign in to the AWS Console and select **US East (N. Virginia) (`us-east-1`)**.
2. Open the **Developer Tools / CodePipeline** console, then **Settings → Connections**.
3. Choose **Create connection** and select **GitHub**.
4. Name the connection **meesho-github** and choose **Connect to GitHub**.
5. Sign in to GitHub when prompted. Select an existing AWS Connector for GitHub installation or install it for the repository owner.
6. Authorize access to the `meesho-lakehouse` repository, then complete the connection in AWS.
7. Check that the connection status is **Available**.
8. Copy the connection ARN. You will paste it into the bootstrap form in the next step.

Both environments can use this same connection. An existing connection is also suitable if it is Available, in this Region, and authorized for this repository. GitHub authorization is a one-time account-owner action; a CloudFormation-created connection would still require this consent step. [AWS connection setup](https://docs.aws.amazon.com/dtconsole/latest/userguide/connections-create-github.html).

## 5. Create the development CI/CD stack in the Console

This is the only template you upload manually for an environment. It replaces the separate foundation, deployment-role and pipeline setup commands from the earlier procedure.

1. Open **CloudFormation → Stacks → Create stack → With new resources (standard)**.
2. Select **Choose an existing template → Upload a template file**.
3. Upload `cloudformation/bootstrap.yaml` from this project.
4. Choose **Next** and enter:

| Form field | Development value |
|---|---|
| Stack name | `meesho-dev-bootstrap` |
| `Environment` | `dev` |
| `Repository` | `YOUR_GITHUB_OWNER/meesho-lakehouse` |
| `ConnectionArn` | The Available connection ARN from step 4 |

5. Continue through stack options. No pre-created CloudFormation execution-role ARN is required for this bootstrap; use the authorized AWS sign-in permissions for this creation.
6. On the review page, acknowledge that the template creates IAM resources with custom names, then submit.
7. Wait for the stack to show **CREATE_COMPLETE**. The **Events** tab shows progress; the **Outputs** tab includes the pipeline name.

The bootstrap creates:

| Resource | Why it exists |
|---|---|
| Encrypted artifact bucket and KMS key | Store pipeline source artifacts, Lambda packages and Glue scripts |
| CodePipeline role | Retrieve GitHub source and start the environment's builds |
| Validation role and CodeBuild project | Run checks before deployment, without deployment permissions |
| Deployment build role and CodeBuild project | Package code and request application stack changes |
| CloudFormation execution role | Create/update the data foundation and application resources, including runtime IAM roles |
| `meesho-dev-delivery` pipeline | Watch the `dev` branch and order validation and deployment |
| Cleanup Lambda, role and log group | Empty buckets and remove generated resources during stack deletion; no processing-time activity |

You do not create any of these resources individually. The bootstrap supplies its bucket, key and role values to CodeBuild automatically.

## 6. Follow the first automatic deployment

1. Open **CodePipeline → Pipelines → meesho-dev-delivery**.
2. The pipeline normally starts when created. If no execution is running, choose **Release change** once to fetch the latest `dev` commit.
3. Watch the stages in order:

| Stage | What runs | What success means |
|---|---|---|
| **Source** | Retrieve the selected `dev` commit through the GitHub connection | AWS has the version of the repository being deployed |
| **Validate** | `buildspec-check.yml` in CodeBuild | Branch/config checks, offline tests and CloudFormation validation passed |
| **Deploy** | `buildspec.yml`, which runs `ops/deploy.py` in CodeBuild | The environment is deployed, its workflow definition is verified, and delivery processing is enabled |

Select a stage's action details to open its CodeBuild logs. The deployment log contains five numbered steps:

1. **Check for an existing platform.** On the first deployment there is nothing to pause. On later deployments, the coordinator blocks new deliveries and lets the active workflow finish before code is replaced.
2. **Deploy the data foundation.** CloudFormation creates or updates `meesho-dev-foundation`, including the data bucket, encryption/versioning settings and immutable-landing policy.
3. **Package the captured source version.** The build uploads the Lambda ZIP, Glue Python files and Athena view SQL under a release ID containing the environment, Git commit and build number.
4. **Deploy the application platform.** CloudFormation creates or updates `meesho-dev-platform` from the checked-in template and passes resource references internally.
5. **Verify readiness and enable processing.** The build verifies the crawler and all three jobs in the workflow, enables the schedule and removes the deployment maintenance gate.

The AWS commands used for these operations execute **inside CodeBuild**. You do not copy those commands into your local terminal.

A green deployment means infrastructure is ready. It does not mean a business delivery has already run: CI does not upload the supplied data or replay it on every code change. The first data upload is step 8.

## 7. Confirm what was deployed

After the development pipeline succeeds, open CloudFormation. These three stacks should exist:

| Stack | Created by | Ownership |
|---|---|---|
| `meesho-dev-bootstrap` | Your one-time Console submission | CI/CD, artifact storage, encryption key and deployment permissions |
| `meesho-dev-foundation` | CodeBuild automatically | Business-data bucket and landing policy |
| `meesho-dev-platform` | CodeBuild automatically | Runtime services and their permissions |

Open `meesho-dev-platform` and review its **Resources** and **Outputs**. It contains the intake and coordinator Lambdas, their IAM permissions, SQS queue and dead-letter queue, manifest EventBridge rule, DynamoDB registry, five-minute scheduler, crawler, Glue workflow/triggers, three Glue jobs, four Glue databases, Athena workgroup, SNS topic/subscription and CloudWatch monitoring.

The foundation's data bucket name includes the environment, account and Region. You can inspect it in the stack output, but you do not need to copy it into an upload command.

At this point the raw Catalog and Iceberg tables can be empty or absent. The crawler discovers source tables when the first delivery arrives; jobs create the Iceberg tables while processing that delivery. Gold creates the Athena views automatically after those tables exist.

If an alert email was configured, confirm the SNS subscription from that mailbox. A pending subscription does not prevent deployment or processing, but it cannot deliver operational emails yet.

## 8. Upload the supplied delivery using AWS CloudShell

CloudShell lets you use the uploader with your current AWS sign-in, without configuring AWS credentials on your computer. It includes Python and pip; the install command below adds only the uploader libraries and selects a Boto3 version compatible with its Python runtime. [AWS CloudShell software](https://docs.aws.amazon.com/cloudshell/latest/userguide/vm-specs.html). Use the same account and Region as the development deployment. The signed-in role must be allowed to read the foundation stack output, upload to its landing prefixes and use the environment's KMS key.

1. In GitHub, select the **dev** branch of `meesho-lakehouse`.
2. Choose **Code → Download ZIP**. This includes the existing data and uploader.
3. In the AWS Console, open **CloudShell** in `us-east-1`.
4. In CloudShell, choose **Actions → Upload file** and select the downloaded `meesho-lakehouse-dev.zip`.
5. Run these commands inside CloudShell:

```bash
unzip meesho-lakehouse-dev.zip
cd meesho-lakehouse-dev
python3 -m pip install --user --upgrade 'boto3>=1.35,<2' 'PyYAML==6.0.3'
python3 ops/upload_delivery.py --environment dev
```

If GitHub named the ZIP differently, use that archive and extracted-folder name. Use a fresh extracted folder when uploading a newer code version.

The final command is the **data upload step only**. It discovers the environment's bucket from CloudFormation, validates local file counts/hashes, uploads each entity file using an immutable key, records its S3 version, and uploads the ready manifest last. It does not create AWS resources, generate data or start Glue directly.

The command prints the `batch_id` and manifest details. Keep the batch ID if you want to inspect that delivery. Uploading the same unchanged manifest again is harmless and does not request a new processing attempt.

For a later delivery, keep the same manifest structure and one entity type per file, then use:

```bash
python3 ops/upload_delivery.py --environment dev --manifest /path/to/delivery/manifest.json
```

If you prefer your local machine, the same uploader works with an already authenticated AWS profile by adding `--profile YOUR_PROFILE --region us-east-1`. This is optional; CloudShell is sufficient for the procedure above.

## 9. Watch processing and query the result

Once the upload finishes, processing is automatic:

1. The S3 ready-manifest event reaches EventBridge and SQS.
2. Intake validates the manifest and creates a `PENDING` batch in DynamoDB.
3. The coordinator's next scheduled tick, normally within five minutes, admits it to the Glue workflow.
4. The **crawler** discovers the source schemas in the Glue Data Catalog.
5. **Bronze** reads those schemas, quarantines invalid envelopes and keeps unique accepted events.
6. **Silver** uses Catalog payload types, records business-rule failures and merges valid latest versions.
7. **Gold** records relationship checks, writes reporting tables, updates the publication pointer and creates/refreshes the Athena views.
8. The coordinator records crawler/job status and releases the completed delivery claim. Quality warnings also produce an SNS notification if a subscriber is confirmed.

Glue startup and processing take additional time after the coordinator admits the batch. A successful data run is separate from the earlier green code-deployment pipeline.

### Inspect progress through the Console

| Console | Select | What to look for |
|---|---|---|
| DynamoDB → Explore items | `meesho-dev-registry` | `BATCH#<batch_id>`; `state`, `quality_status`, `execution_steps`, `workflow_run_id` |
| Glue → Workflows | `meesho-dev-workflow` | Crawler → Bronze → Silver → Gold execution graph |
| Glue → Crawlers / Data Catalog | `meesho-dev-raw-discovery`, `meesho_dev_raw` | Seven `discovered_*` source tables and inferred payload types |
| CloudWatch → Dashboards | `meesho-dev-operations` | Queue backlog and Lambda errors |
| S3 | Development data bucket | `quality/` results, `quarantine/` records, `warehouse/` Iceberg files |

Normal delivery states are `PENDING → STARTING → RUNNING → SUCCEEDED`. Quality failures do not fail the pipeline: invalid input rows are excluded and results are retained. Gold relationship warnings can mean incomplete or inconsistent totals. Storage, permissions, API, Spark or commit failures still mark an execution failed and retain its delivery claim for recovery.

### Query Athena

1. Open **Athena → Query editor**.
2. Select workgroup **meesho-dev-analytics** and data source **AwsDataCatalog**.
3. Select database **meesho_dev_gold**.
4. After the first data run succeeds, the `current_*` views are available automatically.
5. Run:

```sql
SELECT business_date, method, captured_inr, refunded_inr, net_collected_inr
FROM meesho_dev_gold.current_daily_collections
ORDER BY business_date, method;
```

For the unchanged supplied delivery, net collected cash totals **₹24,099,253.00**, and booked order value totals **₹33,596,103.00**. Bronze contains **106,600 distinct events** from **107,100 physical source rows**. Further business queries are in `sql/business_queries.sql`; use the development database prefix in the query editor.

## 10. Make the next development change

Edit code, business rules, `config/dev.yaml`, `cloudformation/foundation.yaml` or `cloudformation/platform.yaml`, then commit and push to `dev`. For example:

```bash
git switch dev
git add .
git commit -m "Update development lakehouse"
git push origin dev
```

The push starts `meesho-dev-delivery` automatically. CodeBuild validates the commit, pauses new admissions, drains any active delivery, updates the foundation and platform, packages the new release, verifies readiness and resumes processing. Existing data is retained. Pending manifests wait through the deployment.

Runtime IAM changes in `platform.yaml` are applied by CloudFormation with the code change. There is no separate manual role-editing step. A failed deployment leaves admissions paused; inspect the failed build before resuming. A later successful pipeline execution removes that maintenance gate automatically.

The one-time CI/CD machinery is owned by `bootstrap.yaml`. Changing the GitHub connection, CI build permissions or the pipeline itself requires updating that bootstrap stack through the Console using the revised template. Ordinary application, data-foundation and runtime-permission changes use Git pushes. The deployment build deliberately does not rewrite its own trust/permission setup.

## 11. Add production in the same AWS account

1. After development succeeds, open a GitHub pull request with **base `main`** and **compare `dev`**.
2. Review and merge the tested changes. This includes both environment configuration files and the shared templates.
3. In CloudFormation, repeat step 5 with the same `bootstrap.yaml`:

| Form field | Production value |
|---|---|
| Stack name | `meesho-prod-bootstrap` |
| `Environment` | `prod` |
| `Repository` | The same GitHub owner/repository |
| `ConnectionArn` | The same Available connection ARN |

4. Open **meesho-prod-delivery**. It uses **Source → Validate → Approve → Deploy**.
5. In the **Approve** stage, review the main-branch commit and the successful development release. Approve it to allow production deployment.
6. Wait for deployment to finish. CloudFormation automatically creates `meesho-prod-foundation` and `meesho-prod-platform`.
7. To load production data, download the **main** branch ZIP, upload/extract it in CloudShell as in step 8, and run:

```bash
python3 ops/upload_delivery.py --environment prod
```

Use `meesho-prod-*` resources and `meesho_prod_*` databases when inspecting production. Its data bucket, registry, workflow, jobs, artifact bucket, roles and publications are separate from development.

For later releases, merge a reviewed pull request from `dev` into `main`, then approve the production pipeline execution. No new stack creation or resource-name copying is needed. A push to `dev` affects development; a push to `main` affects production. Both environments still share account-level quotas.

## 12. Find and recover a problem

Start at the failed stage or delivery; the Console links lead to the underlying reason.

| Symptom | Where to inspect | Next action |
|---|---|---|
| Source fails | Connections status and GitHub installation access | Complete/re-authorize the connection, then retry the pipeline stage |
| Validate fails | Validate action → CodeBuild logs | Correct the indicated code/config/template issue and push again |
| Deploy fails | Deploy build logs and the failing CloudFormation stack's Events | Correct the reported cause and retry; use CloudFormation's recovery options if the stack is in a state that cannot be updated |
| Deploy waits for an active delivery | DynamoDB batch and Glue workflow | Let it finish; resolve an execution-failed delivery before retrying deployment |
| Upload gets AccessDenied | CloudShell identity, foundation/data bucket and KMS access | Use an account role authorized for this project's data upload |
| Batch stays PENDING | Scheduler state, `MAINTENANCE` and `CONTROL` registry items | Finish/retry the code deployment or resolve the active delivery |
| Data run fails | Batch `execution_steps`, Glue run details and CloudWatch logs | Correct the execution cause, then request a retry through the coordinator |
| Data run succeeds with warnings | S3 quality results and quarantine records | Inspect affected rows/keys; submit corrected data with higher source versions when appropriate |
| Athena views are absent | Gold job status and its Athena-view creation logs | Wait for or recover the first successful data run |

### 12.1 Recover a failed first platform deployment

If `meesho-dev-platform` shows **ROLLBACK_FAILED**, its first creation failed and CloudFormation could not finish removing the resources it had created. Read the Events from the earliest `CREATE_FAILED` entry onward. Entries saying **Resource creation cancelled** are consequences; find the resource with a specific failure reason. With the updated `ops/deploy.py`, CodeBuild also prints the current attempt's resource failures, including rollback errors, below the AWS CLI failure.

Two errors in the earlier templates can occur together:

| Event | Cause and correction |
|---|---|
| Crawler: `Invalid grouping table level: 3 ... Valid range {4, 100}` | The crawler target is `s3://bucket/landing/objects/<entity>/`. Glue counts the bucket as level 1, so the entity folder is level 4. The corrected `platform.yaml` uses `TableLevelConfiguration: 4`. |
| Database: `glue:DeleteDatabase ... userDefinedFunction/meesho_dev_.../* ... AccessDenied` | CloudFormation's role covered the catalog, databases and tables but omitted child-function resources. AWS checks those resources when deleting a database. The corrected `bootstrap.yaml` includes the environment's `userDefinedFunction` ARN pattern. |

For this failed first deployment, apply the corrections in this order:

1. In **us-east-1**, open **CloudFormation → meesho-dev-bootstrap → Update stack → Replace current template**. Upload the corrected `cloudformation/bootstrap.yaml` from the cloned `meesho-lakehouse` repository.
2. Keep the existing `Environment`, `Repository` and `ConnectionArn` values. Acknowledge the IAM changes, submit the update, and wait for **UPDATE_COMPLETE**. This updates `meesho-dev-cloudformation`, the role CloudFormation uses to delete the remaining Glue databases. A Git push alone cannot apply this bootstrap permission change.
3. Open **meesho-dev-platform**, confirm this is the failed first deployment, then choose **Delete**. Wait until deletion finishes. This removes the failed platform's remaining resources, including its Glue databases. Keep **meesho-dev-foundation** and **meesho-dev-bootstrap**. If deletion fails, inspect its new Events before retrying; do not retain failed resources or force deletion.
4. Commit the corrected files and push to **dev**. Wait for the new CodePipeline execution to finish **Validate**, then **Deploy**. The deployment reuses the foundation bucket, publishes the code release, and recreates the platform with the corrected crawler. Retry an old pipeline execution only if its source commit already contains these corrections.
5. Confirm **meesho-dev-platform → CREATE_COMPLETE** and a successful Deploy action before uploading a data delivery.

This deletion sequence is for a platform whose **first creation failed**. For an established platform in **UPDATE_ROLLBACK_FAILED**, inspect and correct the reported resource error, use CloudFormation's **Continue update rollback**, and wait for **UPDATE_ROLLBACK_COMPLETE** before rerunning deployment. If another stack operation is still running, wait for it to finish. The deployment script checks platform status before calling the coordinator so an incomplete stack produces a clear recovery message.

### 12.2 Retry a data delivery

For a terminal execution failure after correcting its cause, open **Lambda → meesho-dev-coordinator → Test**, create a test event containing the following, replace the batch ID, and run it:

```json
{"action":"retry","batch_id":"PASTE_THE_BATCH_ID"}
```

The coordinator validates the prior execution and returns the delivery to `PENDING`. With dispatch enabled, the normal schedule starts it. If a deployment paused dispatch, the retry action leaves the batch pending without starting it. Retry the code deployment to install the correction; the successful release then enables processing of the pending batch. The same action can deliberately replay a successful delivery. Preserve original event IDs and versions when replaying unchanged data.

A `NEEDS_REVIEW` batch means the coordinator could not establish whether a workflow start was accepted. Inspect the Glue run history before making a recovery decision. Do not manually delete its ownership record to bypass that uncertainty.

## 13. Existing deployments and ownership

This bootstrap procedure is for a new environment. If you already deployed the earlier separate foundation/deployment-role/delivery stacks, their buckets, keys and roles still belong to those stacks. Creating this bootstrap under the same resource names will not adopt them automatically. Plan a CloudFormation resource-import/migration before replacing that existing setup; this revision does not delete or migrate live AWS resources from the workspace.

All resources in the current templates use `DeletionPolicy: Delete` and `UpdateReplacePolicy: Delete`. Normal in-place deployments preserve existing data. Deleting an environment permanently removes its source files, Iceberg data, quality evidence, delivery registry and code artifacts.

For an environment already using these three stacks, install this deletion support **before** retiring it:

1. Open **CloudFormation → meesho-dev-bootstrap → Update stack → Replace current template**. Upload the revised `cloudformation/bootstrap.yaml`, keep the existing parameter values, acknowledge IAM creation and complete the update. This adds the cleanup helper and its permissions.
2. Push the revised repository to `dev` and wait for **Deploy** to succeed. The pipeline updates foundation and platform with their cleanup callbacks, deletion policies and stack dependencies.
3. Repeat the bootstrap update for production, then merge the tested changes into `main` and approve its pipeline deployment when production needs the same behavior.

A local file edit does not change an already-deployed stack. Deletion follows the template currently installed in AWS. Keep the environment identity (`dev` or `prod`) unchanged when updating bootstrap. Resources retained by an earlier stack deletion, manually copied data, and independent backups are outside these stacks' ownership.

## 14. Delete an environment and its data

Use the AWS Console in **us-east-1**. This is permanent retirement, including all versions of files in both project buckets. Export anything you intend to keep before starting. No terminal exports, bucket-emptying commands or manual IAM deletion are required.

### 14.1 Stop new activity and allow current work to finish

1. Stop pushes/merges to the environment's branch, data uploads and new Athena queries. In **CodePipeline → meesho-dev-delivery**, let all executions finish or stop them, including any queued/approval-waiting executions. Check **CodeBuild**: neither `meesho-dev-validate` nor `meesho-dev-deploy` should have running or queued builds. A deployment must not recreate resources during retirement.
2. Open **Lambda → meesho-dev-coordinator → Test** and run this event. It creates the maintenance gate so pending deliveries cannot start:

```json
{"action":"pause"}
```

3. In **EventBridge Scheduler → Schedules**, disable `meesho-dev-dispatch`. In **EventBridge → Rules**, disable `meesho-dev-manifests`. In **Lambda → meesho-dev-intake → Configuration → Triggers**, select its SQS trigger and disable it. This stops new scheduled/inbound invocations during teardown.
4. Let the current Glue workflow, crawler and individual job runs finish, or stop them through Glue if their output is no longer needed. The crawler must show **Ready**. Wait at least five minutes after disabling invocation sources so any already-running Lambda calls can finish. Let active Athena queries finish or cancel them.

For production, use `meesho-prod-*` names and the `main` branch throughout these steps. Development and production are retired separately.

### 14.2 Delete the three stacks in order

In **CloudFormation → Stacks**, select each stack below and choose **Delete**. Wait for its deletion to finish before selecting the next one.

| Order | Development stack | What deletion removes |
|---|---|---|
| 1 | `meesho-dev-platform` | Runtime IAM roles, intake/coordinator functions, triggers, workflow, crawler, Glue jobs and databases, registry, queues, SNS topic/subscription, Athena workgroup and saved queries, alarms, dashboard and managed logs |
| 2 | `meesho-dev-foundation` | Data bucket and policy; all landing, warehouse, quarantine, quality, temporary and Athena output files, including old versions and delete markers |
| 3 | `meesho-dev-bootstrap` | CodePipeline, CodeBuild projects/history/logs, deployment/build/pipeline roles, artifact bucket and every stored version, key alias and cleanup helper; schedules KMS key deletion |

These are three independent stacks, so deleting bootstrap alone does not delete the application. CloudFormation imports enforce the order: platform imports the foundation bucket and bootstrap helper; foundation imports the bootstrap helper. AWS blocks deleting a stack whose exported value is still in use.

### 14.3 What CloudFormation cleans automatically

- **Platform first:** its cleanup callback pauses admissions again, checks that Glue is idle, and disables point-in-time recovery on the registry immediately before deletion. This avoids DynamoDB creating a final 35-day recovery backup. It removes crawler streams and identifiable legacy job streams from shared Glue log groups. New Glue jobs write to `/aws-glue/meesho-dev/error` and `/aws-glue/meesho-dev/output`, which belong to the platform stack.
- **Buckets:** the callback adds a write-blocking bucket policy for teardown, aborts incomplete multipart uploads, and removes every object version and delete marker. CloudFormation can then delete the empty bucket and its policy. File versioning does not retain data after this purge.
- **Bootstrap:** its callback requires idle CodeBuild projects, removes their build records and generated log groups, and empties artifact storage. CloudFormation then deletes the pipeline, builds, roles and helper. The helper's own log group is removed after its function.
- **Normal deployments:** custom-resource Create/Update requests do no cleanup. A Delete callback caused by an update rollback also does not purge existing data. Cleanup runs during stack deletion or rollback of a first stack creation.

The helper is an inline Lambda in `bootstrap.yaml`, called only by CloudFormation. It is separate from the delivery architecture and performs no job auditing. Its IAM permissions and bucket-name checks limit cleanup to the selected environment; shared Glue log groups are preserved because other projects may use them.

### 14.4 Verify completion and handle a failed deletion

1. Check each stack's **Events** for successful deletion. Deleted stacks can remain visible under CloudFormation's deleted-stack filter as historical records.
2. Verify that the environment's two S3 buckets, registry, Glue databases/jobs/crawler/workflow, Lambda functions, queues, topics, pipeline/build projects, roles, alarms/dashboard and dedicated log groups are gone.
3. In **KMS → Customer managed keys**, the environment key should show **Pending deletion**. The template sets the minimum **seven-day** waiting period. AWS cannot destroy the key immediately; it becomes unusable while waiting. [AWS KMS deletion behavior](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-kms-key.html).
4. If a stack shows **DELETE_FAILED**, read the failing resource's Events. For a cleanup failure, inspect `/aws/lambda/meesho-dev-cleanup`. Finish/stop an active build or Glue run, or correct the reported permission, then choose **Retry delete**. Large histories or buckets can require another attempt after the helper's time budget; completed removals are safe to repeat. Use a normal retry without retaining failed resources or forcing deletion, because those options can leave resources behind. A failed bucket cleanup leaves its deletion-time write block in place until deletion succeeds.

The GitHub repository, GitHub app installation and pre-existing CodeConnections connection are not created by these stacks; the connection may serve both environments. Keep them while another environment uses them. If retiring both environments and the connection is dedicated to this project, remove it separately in **Developer Tools → Settings → Connections**, and revoke repository access in GitHub if no longer needed.

AWS-managed history does not disappear immediately with the resources: CloudWatch metrics expire under AWS retention, and Glue removes database child metadata asynchronously. Account CloudTrail history is also outside these stacks. Shared Glue log groups remain, while the project streams identified from crawler/job history are removed. Older orphaned streams whose execution history has already expired require an ownership review; do not delete an entire shared group. [CloudWatch metric retention](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html), [Glue database deletion](https://docs.aws.amazon.com/glue/latest/webapi/API_DeleteDatabase.html).

For the service behaviors behind the cleanup, see [S3 bucket deletion](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-s3-bucket.html), [DynamoDB deletion with recovery enabled](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery_Howitworks.html), and [CodeBuild project deletion](https://docs.aws.amazon.com/codebuild/latest/userguide/delete-project.html).

## Relevant implementation files

| File | Responsibility |
|---|---|
| `cloudformation/bootstrap.yaml` | One-time Console setup for CI/CD, permissions and the stack-deletion helper |
| `buildspec-check.yml` | Automatic branch checks, tests and template validation |
| `buildspec.yml` / `ops/deploy.py` | Automatic first deployment and subsequent updates |
| `cloudformation/foundation.yaml` | Data bucket and landing policy, deployed by CI |
| `cloudformation/platform.yaml` | All runtime resources, service permissions and optional email subscription |
| `config/dev.yaml`, `config/prod.yaml` | Environment pairing and notification mailbox |
| `ops/upload_delivery.py` | Existing data files and manifest upload; automatic bucket discovery |
| `src/glue/gold.py`, `src/glue/common.py`, `sql/views.sql` | Automatic reporting publication and Athena view preparation |

This runbook describes the implemented workflow. Local checks do not prove that your AWS account permissions, quotas and service integrations have passed; that is established by the first deployment and data delivery in your account.
