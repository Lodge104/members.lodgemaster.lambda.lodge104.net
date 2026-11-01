# lodgemaster-memberdata

Refreshes the Lodge 104 membership roster from the LodgeMaster portal into
DynamoDB on an hourly schedule. Deployed with AWS SAM (`template.yaml`) and
a CodePipeline (`codepipeline.yaml`) that builds, packages, and deploys the
stack to the `prod` region on every push to `main`.

This is the **first stage** of the Lodge 104 membership data pipeline: the
table it writes to (and that table's DynamoDB stream) is the sole input to
the downstream [`sync.mailerlite.lambda.lodge104.net`](https://github.com/Lodge104/sync.mailerlite.lambda.lodge104.net)
stack, which keeps MailerLite subscribers in sync with membership changes.

## Architecture

```
lodge104-lodgemaster-scrapper (separate repo/stack)
        │ refreshes session cookies, hourly
        ▼
lodge104-keys (DynamoDB table, owned by the scrapper's stack)
        │
        ▼
lodge104-memberdata (this function) ──hourly, dedicated EventBridge rule──▶
        │
        ▼
lodge104-member_data (DynamoDB table + stream, owned by this stack)
        │
        ▼
 ...consumed by the separate lodge104-mailerlite-sync stack...
```

Every hour, a dedicated EventBridge schedule rule (owned by this stack)
invokes `MemberDataFunction`. It reads short-lived LodgeMaster portal
session cookies from the `lodge104-keys` DynamoDB table (refreshed on the
same cadence, out-of-band, by the separately-owned
`lodge104-lodgemaster-scrapper` function), uses them to call the
LodgeMaster `MemberGrid` OData endpoint, and writes every returned member
record into the `lodge104-member_data` DynamoDB table via
`batch_writer()`.

That table's stream (`NEW_AND_OLD_IMAGES`) is read by an EventBridge Pipe
(managed entirely outside this stack) that forwards filtered change events
to the `lodge104-mailerlite-sync` stack's SQS queue, which keeps MailerLite
subscribers current.

## Session cookies, not a bearer token

Unlike `lodge104-mailerlite-sync` (which authenticates with a single SSM
`SecureString` bearer token), this function authenticates to the
LodgeMaster portal using four session cookies -- `XSRF-TOKEN`,
`ai_session`, `OA.LM.Lodge.Auth`, and `ai_user` -- stored as items in the
`lodge104-keys` DynamoDB table. That table is **not** owned by this stack:
it belongs to the `lodge104-lodgemaster-scrapper` function/stack, which
logs into the portal and refreshes those four items hourly.

`MemberDataRole`'s `ReadLodgeMasterSessionCookies` policy grants
`dynamodb:GetItem` on that table scoped, via a `dynamodb:LeadingKeys`
condition, to exactly those four item names -- it cannot read any other
item in `lodge104-keys`. The cookie values themselves are fetched once per
execution environment (cached across warm invocations) and are never
logged; only the four item *names* (which are not secrets) appear in code
or CloudWatch Logs.

If `lodge104-lodgemaster-scrapper` stops running, or any of the four items
go missing, `lambda_function.py` raises immediately with a message naming
the missing item -- which should also trip `MemberDataErrorAlarm` (see
below).

## Monitoring & alerts

The stack defines a CloudWatch alarm, `MemberDataErrorAlarm`, that watches
the `Errors` metric (`AWS/Lambda` namespace) for `MemberDataFunction`. By
default it fires when 2 of the last 3 hourly invocation windows had 1 or
more failed invocations
(`ErrorAlarmThreshold`/`ErrorAlarmPeriodSeconds`/`ErrorAlarmEvaluationPeriods`/`ErrorAlarmDatapointsToAlarm`).
It returns to `OK` once that many consecutive windows fall back under the
threshold; a window with no error datapoint is treated as healthy. Both the
`ALARM` and `OK` transitions publish to the SNS topic referenced by the
`AlarmTopicArn` parameter.

That topic -- `lodge104-lambda` -- is **shared** across other Lodge 104
Lambda functions (including `lodge104-mailerlite-sync`, not just this one)
and already has email, SMS, and Slack subscribers. For that reason, the
topic itself is **not** defined/owned by this stack; it's only referenced
by ARN. Managing subscriptions (adding or removing recipients) is done
directly on the topic outside of this repo, so that other stacks publishing
to the same topic are unaffected.

To point at a different notification topic (e.g. for a non-prod stack),
override the `AlarmTopicArn` parameter at deploy time.

### One-time import for existing resources

The IAM role (`lodge104-memberdata-role-94rpi1nc`), the Lambda function
(`lodge104-memberdata`), and the DynamoDB table (`lodge104-member_data`)
all pre-date this stack and were created directly in the console in 2023.
A normal deployment can overwrite an existing resource with the same
name/physical ID, including live configuration. Import each resource when
you need to adopt and preserve its live configuration rather than allowing
a normal deployment to reconcile it with the template.

Before the first import, the live IAM role also carries the
`AWSLambdaInvocation-DynamoDB` and `AmazonDynamoDBFullAccess` managed
policies. Those are dropped by `template.yaml` in favor of the scoped
inline policies on `MemberDataRole`; the import itself only adopts the
role's identity (name/path/trust policy), and the first subsequent stack
update is what actually narrows its permissions.

`MemberDataErrorAlarm` is a brand-new resource -- no alarm previously
existed for this function -- so it is not an import target; it's created
normally the first time the stack deploys.

**Trigger migration.** `lodge104-memberdata` is currently invoked by a
pre-existing, shared EventBridge rule named `Hourly`
(`rate(1 hour)`), which also targets the unrelated
`lodge104-lodgemaster-scrapper` function. This stack does **not** import or
own that shared rule -- doing so would make deleting this stack delete the
scrapper's trigger too. Instead, `template.yaml` gives
`MemberDataFunction` its own dedicated EventBridge schedule rule
(`lodge104-memberdata-hourly`). Before the first deploy, remove this
function from the shared rule's targets so it isn't invoked twice an hour:

```bash
aws events remove-targets \
  --rule Hourly \
  --ids mwu0kii14vpadzzbe373 \
  --region us-east-1
```

(`lodge104-lodgemaster-scrapper`'s own target on that rule is untouched.)

The CodePipeline deploys on every push to `main`. Before merging a change
that imports these resources, pause deployment by disabling the inbound
transition to `DeployProd`, and ensure no deployment is already in
progress:

```bash
aws codepipeline disable-stage-transition \
  --pipeline-name <pipeline-name> \
  --stage-name DeployProd \
  --transition-type Inbound \
  --reason "Import pre-existing resources before deployment"
```

Package the template and use the packaged template for the import change
set:

```bash
sam build --template-file template.yaml
sam package \
  --template-file .aws-sam/build/template.yaml \
  --s3-bucket <pipeline-artifact-bucket> \
  --region us-east-1 \
  --output-template-file packaged-prod.yaml

aws cloudformation create-change-set \
  --stack-name lodge104-memberdata \
  --change-set-type IMPORT \
  --change-set-name import-existing-resources \
  --template-body file://packaged-prod.yaml \
  --parameters <existing-parameter-overrides> \
  --resources-to-import \
    '[{"ResourceType":"AWS::IAM::Role","LogicalResourceId":"MemberDataRole","ResourceIdentifier":{"RoleName":"lodge104-memberdata-role-94rpi1nc"}},
      {"ResourceType":"AWS::DynamoDB::Table","LogicalResourceId":"MemberDataTable","ResourceIdentifier":{"TableName":"lodge104-member_data"}},
      {"ResourceType":"AWS::Lambda::Function","LogicalResourceId":"MemberDataFunction","ResourceIdentifier":{"FunctionName":"lodge104-memberdata"}}]' \
  --capabilities CAPABILITY_NAMED_IAM

aws cloudformation execute-change-set \
  --stack-name lodge104-memberdata \
  --change-set-name import-existing-resources

aws codepipeline enable-stage-transition \
  --pipeline-name <pipeline-name> \
  --stage-name DeployProd \
  --transition-type Inbound
```

Resume deployment only after the import succeeds. Subsequent pipeline runs
manage these resources like any other stack resource. `lodge104-keys` and
the `lodge104-lodgemaster-scrapper` function are never imported or deleted
by this stack -- they are only read (by name/ARN), and are owned by a
separate repo/stack. Likewise, the `lodge104-mailerlite-sync` stack's
EventBridge Pipe and SQS queue downstream of `lodge104-member_data`'s
stream are entirely outside this stack's concern.

### Resource sizing notes

- `MemorySize` (3008 MB) and `Timeout` (900s, the Lambda maximum) are kept
  at their live, pre-existing values: the handler parses a JSON response
  covering the full ~10k-member roster and batch-writes it to DynamoDB.
- `EphemeralStorage` is reduced from the live console default of 3008 MB to
  the 512 MB service default, since the handler never writes to `/tmp`.
- `Runtime` is bumped from the live `python3.10` to `python3.12`, matching
  the python toolchain already pinned in the CodeBuild buildspecs, as part
  of this SAM migration.
- Unlike `lodge104-mailerlite-sync`'s `SyncFunction`, `MemberDataFunction`
  has no `ReservedConcurrentExecutions`: it's invoked by a single hourly
  schedule rule with no fan-out, so there's no concurrent-invocation
  concern to cap.

## Local MCP servers

The workspace AWS MCP server uses `mcp-proxy-for-aws-cli==1.7.0`, pinned to
avoid resolving a moving package version at startup. The proxy uses the
workspace user's AWS CLI credentials. Review and explicitly approve the
package publisher, source, and requested AWS access before starting this
server; pinning the version does not replace that trust review.

## Deploying the pipeline

```bash
sam deploy -t codepipeline.yaml --stack-name lodge104-memberdata-pipeline \
  --capabilities CAPABILITY_NAMED_IAM --region us-east-1
```

The pipeline reuses the GitHub CodeStar Connection already authorized for
the Lodge104 org (see `CodeStarConnectionArn`'s default in
`codepipeline.yaml`, shared with the `lodge104-mailerlite-sync` pipeline).
If that connection does not have access to this repository, override
`CodeStarConnectionArn` with an empty string to have the stack create and
own its own connection instead; that one starts in `PENDING` status and
needs a one-time GitHub OAuth handshake in the console.

Its self-mutation role (`PipelineStackCloudFormationExecutionRole`) cannot
modify IAM roles or policies; changes to pipeline IAM permissions require a
separately approved stack update outside the self-mutating `UpdatePipeline`
stage.