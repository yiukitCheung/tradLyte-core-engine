# AWS Step Functions Pipeline Orchestration

## Overview

This directory contains the AWS Step Functions state machine that orchestrates the daily OHLCV data pipeline. The state machine is created/updated by `deploy_step_functions.sh` and triggered Mon–Fri by an EventBridge Scheduler entry.

All resource names below match exactly what `deploy_step_functions.sh` provisions. If you change a name there, update this document.

## Architecture

```
                Step Functions: dev-daily-ohlcv-pipeline
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 0: PLAN                                          (~10 sec) │    │
│   │  Lambda: dev-batch-daily-ohlcv-planner  (VPC)                    │    │
│   │    · reads data_ingestion_watermark (SCD Type 2)                 │    │
│   │    · derives missing dates                                       │    │
│   │    · fans out fetcher invokes                                    │    │
│   │      - multi-date  → async per-date invokes                      │    │
│   │      - single-date → synchronous invoke (returns dates_processed)│    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 1: PARALLEL FETCHERS                            (~3 min)  │    │
│   │  Lambda: dev-batch-daily-ohlcv-fetcher  (no VPC)                 │    │
│   │  Lambda: dev-batch-daily-meta-fetcher   (no VPC)                 │    │
│   │    → S3 bronze/raw_ohlcv/  +  bronze/raw_meta/                   │    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 2: INGEST                                       (~1–2 min)│    │
│   │  Lambda: dev-batch-daily-ohlcv-ingest-handler  (VPC)             │    │
│   │  Lambda: dev-batch-daily-meta-ingest-handler   (VPC)             │    │
│   │    · OHLCV: parquet → RDS upsert + watermark update              │    │
│   │    · Meta : manifest → symbol_metadata upsert                    │    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 3: PARTITION SYMBOLS                          (~30 sec)   │    │
│   │  Lambda: dev-batch-scan-partitioner  (VPC)                       │    │
│   │    · queries RDS once for active symbols                         │    │
│   │    · writes 10 chunk_N.json files to S3 (scanner-chunks/{date}/) │    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 4: SCANNER WORKERS  (Array Job × 10)        (~10–20 min)  │    │
│   │  AWS Batch on Fargate: dev-batch-scanner-worker  (4 vCPU / 8 GB) │    │
│   │  Each container:                                                 │    │
│   │    1. Downloads its S3 chunk (≈500 symbols)                      │    │
│   │    2. Loads OHLCV from RDS                                       │    │
│   │    3. Runs trading strategies                                    │    │
│   │    4. Writes signals → daily_scan_signals (RDS staging)          │    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 5: SCANNER AGGREGATOR                         (~1–2 min)  │    │
│   │  AWS Batch on Fargate: dev-batch-scanner-aggregator (2 vCPU/4 GB)│    │
│   │    1. Reads all signals from daily_scan_signals                  │    │
│   │    2. Global rank across full universe                           │    │
│   │    3. Writes top picks → stock_picks                             │    │
│   │    4. Cleans up daily_scan_signals for today                     │    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│                    ┌─────────────────────────┐                             │
│                    │  Pipeline Complete       │  (~15–25 min total)        │
│                    └─────────────────────────┘                             │
│                                                                            │
│   ON FAILURE (any stage) → SNS: condvest-pipeline-alerts → Email          │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

## AWS resources

| Resource | Name | Created by | Description |
|---|---|---|---|
| **State Machine** | `dev-daily-ohlcv-pipeline` | `deploy_step_functions.sh` | Main orchestration workflow |
| **IAM Role (state machine)** | `dev-step-functions-pipeline-role` | `deploy_step_functions.sh` | Lambda invoke + Batch submit + SNS publish |
| **IAM Role (scheduler)** | `dev-eventbridge-stepfunctions-role` | `deploy_step_functions.sh` | Lets EventBridge Scheduler call `states:StartExecution` |
| **EventBridge Schedule** | `dev-daily-ohlcv-pipeline-schedule` | `deploy_step_functions.sh` | Cron `5 16 ? * MON-FRI *` in `America/New_York` (4:05 PM ET) |
| **SNS Topic** | `condvest-pipeline-alerts` | `deploy_step_functions.sh` | Failure notifications |
| **Lambda — plan** | `dev-batch-daily-ohlcv-planner` | `infrastructure/fetching/deploy_lambda.sh` | Stage 0 |
| **Lambda — OHLCV fetch** | `dev-batch-daily-ohlcv-fetcher` | `infrastructure/fetching/deploy_lambda.sh` | Stage 1 (no VPC) |
| **Lambda — meta fetch** | `dev-batch-daily-meta-fetcher` | `infrastructure/fetching/deploy_lambda.sh` | Stage 1 (no VPC) |
| **Lambda — OHLCV ingest** | `dev-batch-daily-ohlcv-ingest-handler` | `infrastructure/ingesting/deploy_lambda.sh` | Stage 2 (VPC) |
| **Lambda — meta ingest** | `dev-batch-daily-meta-ingest-handler` | `infrastructure/ingesting/deploy_lambda.sh` | Stage 2 (VPC) |
| **Lambda — partitioner** | `dev-batch-scan-partitioner` | `infrastructure/processing/lambda_functions/deploy_processing_lambda.sh` | Stage 3 |
| **Batch Job Queue** | `dev-batch-scanner` | `infrastructure/processing/batch_job/deploy_scanner_batch_jobs.sh` | Fargate queue for scanner jobs |
| **Batch Job Definition** | `dev-batch-scanner-worker` | `infrastructure/processing/batch_job/deploy_scanner_batch_jobs.sh` | Stage 4 (Array × 10, 4 vCPU / 8 GB) |
| **Batch Job Definition** | `dev-batch-scanner-aggregator` | `infrastructure/processing/batch_job/deploy_scanner_batch_jobs.sh` | Stage 5 (single, 2 vCPU / 4 GB) |
| **CloudWatch Log Group** | `/aws/batch/dev-batch-scanner` | created on first run | Scanner job logs |

## Schedule

| Component | Time (`America/New_York`) |
|---|---|
| Pipeline trigger | 4:05 PM (Mon–Fri) |
| Expected completion | ~4:25–4:30 PM |

Total duration: ~15–25 minutes (plan + fetchers ~3 min + ingest ~1–2 min + partitioner ~30 sec + scanner Array Job ~10–20 min + aggregator ~1–2 min). The EventBridge Scheduler entry uses a cron expression with `America/New_York` so the trigger time stays stable across DST changes.

## Key benefits

- **Parallel execution** — OHLCV and metadata fetchers run in parallel; 10 scanner workers run simultaneously.
- **Automatic retries** — Lambda stages retry twice with exponential backoff; Batch stages retry twice with backoff.
- **Failure alerts** — SNS notification with full error details on any stage failure.
- **Visual monitoring** — Step Functions console shows real-time execution graph per stage.
- **Efficient RDS access** — partitioner queries symbols once; each worker only touches its ~500-symbol slice.
- **Global ranking** — aggregator ranks across the full universe after all workers finish.
- **Replayable** — S3 bronze is the source of truth; ingest can re-run idempotently from S3 (`ON CONFLICT DO UPDATE`).

## Files

| File | Description |
|---|---|
| `state_machine_definition.json` | Step Functions ASL definition |
| `deploy_step_functions.sh` | Deploys state machine + IAM roles + SNS + EventBridge Scheduler |
| `README.md` | This file |

## Related deployment scripts

| Script | Location | Description |
|---|---|---|
| `deploy_lambda.sh` | `infrastructure/fetching/` | Deploys planner + OHLCV/meta fetchers |
| `deploy_lambda.sh` | `infrastructure/ingesting/` | Deploys OHLCV + meta ingest handlers |
| `deploy_processing_lambda.sh` | `infrastructure/processing/lambda_functions/` | Deploys `scan_partitioner` Lambda |
| `build_scanner_container.sh` | `infrastructure/processing/batch_job/` | Builds + pushes scanner Docker image to ECR |
| `deploy_scanner_batch_jobs.sh` | `infrastructure/processing/batch_job/` | Registers worker + aggregator job definitions and the queue |
| `wire_scanner_to_rds_proxy.sh` | `infrastructure/processing/batch_job/` | Switches scanner DSN to the RDS Proxy endpoint |
| `create_secretsmanager_vpc_endpoint.sh` | `infrastructure/common/` | Provisions the Secrets Manager interface endpoint used by VPC Lambdas |

## Manual operations

> Replace `<ACCOUNT_ID>` and `<REGION>` (default region is `ca-west-1`) with your own values.

### Trigger pipeline manually

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:<REGION>:<ACCOUNT_ID>:stateMachine:dev-daily-ohlcv-pipeline" \
  --name "manual-$(date +%Y%m%d%H%M%S)" \
  --region <REGION>
```

**Pinning `scan_date`:** the partitioner only includes symbols that already have `raw_ohlcv` for the chosen day. By default `scan_date` is the **UTC calendar date** of the execution start. If you start a run after midnight UTC (e.g. 04:01 UTC on Wed) but the latest bars are still for Tue in the US session, pass the OHLCV date explicitly so the scanner matches RDS:

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:<REGION>:<ACCOUNT_ID>:stateMachine:dev-daily-ohlcv-pipeline" \
  --name "manual-scan-$(date +%Y%m%d%H%M%S)" \
  --region <REGION> \
  --input '{"scan_date":"2026-04-28"}'
```

If the partitioner finds no symbols for that date (`chunks_written: 0`), the state machine **skips** the Batch scanner jobs instead of failing ten workers.

### Check pipeline status

```bash
aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:<REGION>:<ACCOUNT_ID>:stateMachine:dev-daily-ohlcv-pipeline" \
  --max-results 5 \
  --region <REGION>

aws stepfunctions describe-execution \
  --execution-arn "arn:aws:states:<REGION>:<ACCOUNT_ID>:execution:dev-daily-ohlcv-pipeline:<EXECUTION_NAME>" \
  --region <REGION>
```

### Disable / enable the daily schedule

```bash
aws scheduler update-schedule \
  --name dev-daily-ohlcv-pipeline-schedule \
  --state DISABLED \
  --region <REGION>

aws scheduler update-schedule \
  --name dev-daily-ohlcv-pipeline-schedule \
  --state ENABLED \
  --region <REGION>
```

> The trigger uses **EventBridge Scheduler** (`aws scheduler …`), not classic EventBridge rules — `aws events disable-rule` will not affect it.

## Monitoring

### AWS Console

1. **Step Functions Console** — visual execution graph showing each stage's progress / status.
2. **CloudWatch Logs** — per-Lambda log groups, plus `/aws/batch/dev-batch-scanner` for scanner jobs.
3. **EventBridge Scheduler Console** — view the daily trigger and its run history.

### SNS alerts

The `condvest-pipeline-alerts` SNS topic publishes notifications when any stage fails. To receive alerts:

1. Open AWS SNS Console → Topics → `condvest-pipeline-alerts`
2. Create subscription
3. Protocol: Email; Endpoint: your email address
4. Confirm the subscription email

## Retry logic

| Stage | Component | Max retries | Timeout (per attempt) | Backoff |
|---|---|---|---|---|
| 0 | Plan Lambda (`dev-batch-daily-ohlcv-planner`) | 2 | 15 min | 30s exponential |
| 1 | OHLCV + meta fetchers (parallel) | 2 | 15 min each | 60s exponential |
| 2 | OHLCV + meta ingest handlers | 2 | 5 min each | 30s exponential |
| 3 | Partition symbols Lambda | 2 | 1 min | 30s exponential |
| 4 | Scanner workers (Array × 10) | 2 | 60 min | 60s |
| 5 | Scanner aggregator | 2 | 20 min | 30s |

Exact retry settings live in `state_machine_definition.json`; the table above documents the intent.

## Troubleshooting

### Pipeline failed — how to investigate

1. Open the **Step Functions Console** and select the failed execution.
2. Click on the red (failed) state in the visual graph.
3. Expand **Error** to see the error message and cause.
4. Open the corresponding CloudWatch log group:
   - Lambda: `/aws/lambda/<function-name>` (e.g. `/aws/lambda/dev-batch-daily-ohlcv-fetcher`)
   - Batch: `/aws/batch/dev-batch-scanner` (filter by job ID)

### Common issues

| Issue | Likely cause | Resolution |
|---|---|---|
| Lambda timeout in fetcher | Polygon throttling or large date span | Lower `max_backfill_days` in planner input; check Polygon tier limits |
| Ingest handler `AccessDeniedException` on Secrets Manager | Lambda role does not include the configured `RDS_SECRET_ARN`, or the Secrets Manager VPC endpoint is missing | Verify role policy + run `infrastructure/common/create_secretsmanager_vpc_endpoint.sh` |
| Batch job fails immediately | Container error / image pull failure | Inspect the relevant `/aws/batch/dev-batch-scanner` log stream by job ID |
| `chunks_written: 0` in partitioner | No `raw_ohlcv` rows for that `scan_date` yet | Pass `scan_date` explicitly (see Manual Operations) or wait for ingest to land |
| SNS alert never arrives | No subscription on `condvest-pipeline-alerts` | Add an email subscription |

## Deployment

To create or update the Step Functions pipeline (state machine + IAM + SNS + scheduler):

```bash
cd cloud/batch_layer/infrastructure/orchestration
./deploy_step_functions.sh
```

The script will:

1. Create / reuse the SNS topic `condvest-pipeline-alerts`.
2. Create / reuse the IAM role `dev-step-functions-pipeline-role` and attach Lambda-invoke / Batch-submit / SNS-publish policies.
3. Create / update the state machine `dev-daily-ohlcv-pipeline` (with X-Ray tracing enabled).
4. Create / reuse the EventBridge Scheduler role `dev-eventbridge-stepfunctions-role`.
5. Create / update the schedule `dev-daily-ohlcv-pipeline-schedule` (cron `5 16 ? * MON-FRI *`, `America/New_York`).

---

**Last updated:** May 2026
