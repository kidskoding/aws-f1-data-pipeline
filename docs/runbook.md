# Runbook

How to operate the pipeline: deploy it, run it, fix it, and remove it.

## Deploy

```bash
make deploy
```

Builds `build/lambda.zip`, runs `terraform apply` (it shows the plan and asks before changing anything), and saves the outputs to `build/terraform-outputs.json` for the other commands.

Optional settings go in `infra/terraform.tfvars` (copy `terraform.tfvars.example`):

| Variable | Default | Meaning |
|---|---|---|
| `notification_email` | empty | Where SNS sends success and failure emails. Confirm the subscription email AWS sends. |
| `schedule_enabled` | `false` | Turn on the weekly run. |
| `redshift_base_capacity` | `4` | Warehouse size in RPUs. |
| `redshift_daily_rpu_hours` | `2` | Daily compute cap before the warehouse turns itself off. |

## Run

| Goal | Command |
|---|---|
| Backfill every season | `make run SEASONS=2021-current` |
| Refresh the current season | `make run` |
| Reload specific seasons | `make run SEASONS=2023,2025` |

The transform always rebuilds the curated zone from everything in `raw/`, so a partial extract never leaves the warehouse with a mix of old and new rules.

## When a run fails

1. **Read the failure email** (or `make run`'s output). It contains the failing step's error.
2. **Open the execution** in the Step Functions console. The failed state is red; its input and error are on the right.
3. **By step:**

| Failed step | Look at | Usual cause |
|---|---|---|
| `Extract` | `/aws/lambda/f1-pipeline-extract` logs | API down or rate limited (it retries 5 times with backoff). Rerun later. |
| `Transform` | Glue console, job run, then "Output logs" | Data Quality rule failed, or row counts didn't reconcile. The log lists each rule's outcome. |
| `LoadWarehouse` | `/aws/lambda/f1-pipeline-load` logs | COPY error. Run `SELECT * FROM sys_load_error_detail ORDER BY start_time DESC LIMIT 10;` in Redshift. |
| `Reconcile` | the email's `Cause` | Lake and warehouse row counts differ. The load is transactional, so the warehouse still holds the previous good data. |

4. **Check what was rejected** in Athena (saved query `rejected-rows-by-reason`):

```sql
SELECT reject_reason, COUNT(*) FROM f1_pipeline_curated.rejected_results GROUP BY 1;
```

5. **Rerun.** Every step is idempotent: the extract replaces a season's raw files, the transform overwrites curated data, and the load replaces tables in one transaction.

## Logs

All Lambda logs are JSON. Example CloudWatch Logs Insights query:

```
fields time, level, message
| filter level = "ERROR" or level = "WARNING"
| sort time desc
```

## Costs to watch

- Redshift Serverless bills per second while queries run (60-second minimum per query). The daily cap stops runaway usage.
- If the weekly schedule is on, each run costs a few cents.
- The AWS Budgets alert on the account emails at $1 and $5.

## Tear down

```bash
make teardown
```

Runs `terraform destroy`. The bucket has `force_destroy = true`, so its contents are deleted too. Turn that off before using this for data you need to keep.
