# 🎯 AWS Step Functions Pipeline Orchestration

## Overview

This directory contains the AWS Step Functions state machine that orchestrates the daily OHLCV data pipeline. The pipeline is fully automated and runs daily after market close.

## Architecture

```
                     Step Functions: condvest-daily-ohlcv-pipeline
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 1: PARALLEL FETCHERS                          (~3 min)    │    │
│   │  ┌─────────────────────────┐   ┌─────────────────────────┐      │    │
│   │  │   Lambda: OHLCV Fetcher │   │  Lambda: Meta Fetcher   │      │    │
│   │  │   (2 retries)           │   │  (2 retries)            │      │    │
│   │  └───────────┬─────────────┘   └───────────┬─────────────┘      │    │
│   │              └─────────────┬───────────────┘                     │    │
│   └────────────────────────────┼─────────────────────────────────────┘    │
│                                ▼                                           │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 2: PARTITION SYMBOLS                          (~30 sec)   │    │
│   │  Lambda: scan_partitioner                                        │    │
│   │  → queries RDS once for active symbols                           │    │
│   │  → writes 10 chunk_N.json files to S3                            │    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 3: SCANNER WORKERS (Array Job x10)         (~10–20 min)   │    │
│   │  Batch: dev-batch-scanner-worker  (4 vCPU / 8 GB each)          │    │
│   │  Each container:                                                 │    │
│   │    1. Downloads its S3 chunk (≈500 symbols)                      │    │
│   │    2. Loads OHLCV from RDS                                       │    │
│   │    3. Runs trading strategies                                    │    │
│   │    4. Writes signals → daily_scan_signals (staging)              │    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  STAGE 4: SCANNER AGGREGATOR                         (~1–2 min)  │    │
│   │  Batch: dev-batch-scanner-aggregator  (2 vCPU / 4 GB)           │    │
│   │    1. Reads all signals from daily_scan_signals                  │    │
│   │    2. Global rank across full universe                           │    │
│   │    3. Writes top picks → stock_picks                             │    │
│   │    4. Cleans up daily_scan_signals for today                     │    │
│   └──────────────────────────────┬───────────────────────────────────┘    │
│                                  ▼                                         │
│                    ┌─────────────────────────┐                             │
│                    │  ✅ Pipeline Complete     │  (~15–25 min total)        │
│                    └─────────────────────────┘                             │
│                                                                            │
│   ON FAILURE (any stage) → SNS: condvest-pipeline-alerts → Email          │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

## AWS Resources

| Resource | Name | Description |
|----------|------|-------------|
| **State Machine** | `condvest-daily-ohlcv-pipeline` | Main orchestration workflow |
| **IAM Role** | `condvest-pipeline-step-functions-role` | Permissions for Lambda/Batch/SNS |
| **EventBridge Scheduler** | `dev-daily-ohlcv-pipeline-schedule` | Daily trigger at 4:05 PM America/New_York |
| **SNS Topic** | `condvest-pipeline-alerts` | Failure notifications |
| **Lambda** | `dev-batch-scan-partitioner` | Symbol partitioner (Stage 2) |
| **Batch Job Queue** | `dev-batch-scanner` | Fargate queue for scanner jobs |
| **Batch Job Def** | `dev-batch-scanner-worker` | Array Job child (4 vCPU / 8 GB) |
| **Batch Job Def** | `dev-batch-scanner-aggregator` | Single aggregator job (2 vCPU / 4 GB) |
| **CloudWatch Log Group** | `/aws/batch/dev-batch-scanner` | Scanner job logs |

## Schedule

| Component | Time (America/New_York) |
|-----------|--------------------------|
| Pipeline Trigger | 4:05 PM (Mon-Fri) |
| Expected Completion | ~4:25–4:30 PM |

**Total Duration:** ~15–25 minutes (fetchers ~3 min + partitioner ~30 sec + scanner array job ~10–20 min + aggregator ~1–2 min). Using `America/New_York` keeps the trigger time stable across DST changes.

## Key Benefits

- **⚡ Parallel Execution:** OHLCV and Metadata fetchers run in parallel (Stage 1); 10 scanner workers run simultaneously (Stage 3)
- **🔄 Automatic Retries:** Lambda stages (2 retries); Batch stages (2 retries with backoff)
- **📧 Failure Alerts:** SNS notification with full error details on any stage failure
- **📊 Visual Monitoring:** AWS Console shows real-time execution graph per stage
- **💾 Efficient RDS Access:** Partitioner queries symbols once; each worker only touches its ~500-symbol slice
- **🏆 Global Ranking:** Aggregator ranks across the full 5,000+ symbol universe after all workers finish

## Files

| File | Description |
|------|-------------|
| `state_machine_definition.json` | Step Functions ASL definition (4-stage pipeline) |
| `deploy_step_functions.sh` | Deployment script for state machine + IAM + EventBridge |
| `README.md` | This documentation |

## Related Deployment Scripts

| Script | Location | Description |
|--------|----------|-------------|
| `deploy_processing_lambda.sh` | `infrastructure/processing/lambda_functions/` | Deploys `scan_partitioner` Lambda |
| `build_scanner_container.sh` | `infrastructure/processing/batch_job/` | Builds + pushes scanner Docker image to ECR |
| `deploy_scanner_batch_jobs.sh` | `infrastructure/processing/batch_job/` | Registers worker + aggregator job definitions |

## Manual Operations

### Trigger Pipeline Manually

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:ca-west-1:471112909340:stateMachine:condvest-daily-ohlcv-pipeline" \
  --name "manual-$(date +%Y%m%d%H%M%S)" \
  --region ca-west-1
```

**Trading session date (`scan_date`):** The partitioner only includes symbols that already have `raw_ohlcv` for the chosen day. By default, `scan_date` is the **UTC calendar date** of the execution start. If you start a run after midnight UTC (e.g. 04:01 UTC on Wed) but your latest bars are still for **Tue** in the US session, pass the OHLCV date explicitly so the scanner matches RDS:

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:ca-west-1:471112909340:stateMachine:condvest-daily-ohlcv-pipeline" \
  --name "manual-scan-$(date +%Y%m%d%H%M%S)" \
  --region ca-west-1 \
  --input '{"scan_date":"2026-04-28"}'
```

If the partitioner finds no symbols for that date (`chunks_written: 0`), the state machine **skips** Batch scanner jobs instead of failing ten workers.

### Check Pipeline Status

```bash
# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn "arn:aws:states:ca-west-1:471112909340:stateMachine:condvest-daily-ohlcv-pipeline" \
  --max-results 5 \
  --region ca-west-1

# Get execution details
aws stepfunctions describe-execution \
  --execution-arn "arn:aws:states:ca-west-1:471112909340:execution:condvest-daily-ohlcv-pipeline:EXECUTION_NAME" \
  --region ca-west-1
```

### Disable/Enable Daily Schedule

```bash
# Disable (pause the pipeline)
aws events disable-rule \
  --name condvest-daily-pipeline-trigger \
  --region ca-west-1

# Enable (resume the pipeline)
aws events enable-rule \
  --name condvest-daily-pipeline-trigger \
  --region ca-west-1
```

## Monitoring

### AWS Console

1. **Step Functions Console:** Visual execution graph showing each stage's progress/status
2. **CloudWatch Logs:** Detailed logs from Lambda and Batch jobs
3. **EventBridge Console:** View scheduled triggers and invocation history

### SNS Alerts

The `condvest-pipeline-alerts` SNS topic sends notifications when any stage fails. To receive alerts:

1. Go to AWS SNS Console → Topics → `condvest-pipeline-alerts`
2. Click "Create subscription"
3. Protocol: Email
4. Endpoint: Your email address
5. Confirm the subscription email

## Retry Logic

| Stage | Component | Max Retries | Timeout | Backoff |
|-------|-----------|-------------|---------|---------|
| 1 | Lambda Fetchers (x2 parallel) | 2 | 15 min each | 60s exponential |
| 2 | Partition Symbols Lambda | 2 | 5 min | 30s exponential |
| 3 | Scanner Workers (Array x10) | 2 | 60 min | 60s interval |
| 4 | Scanner Aggregator | 2 | 20 min | 30s interval |

## Troubleshooting

### Pipeline Failed - How to Investigate

1. Go to **Step Functions Console** → Select the failed execution
2. Click on the **red (failed) state** in the visual graph
3. Expand **"Error"** to see the error message
4. Check **CloudWatch Logs** for detailed stack traces:
   - Lambda: `/aws/lambda/daily-ohlcv-fetcher`
   - Batch: `/aws/batch/job` (search for job ID)

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Lambda timeout | Too many symbols to fetch | Check Polygon API rate limits |
| Batch job FAILED | Container error | Check CloudWatch logs for job ID |
| Consolidator error | Schema mismatch | Verify data.parquet schema matches date files |
| SNS not received | No subscription | Add email subscription to SNS topic |

## Deployment

To deploy or update the Step Functions pipeline:

```bash
cd /path/to/data_pipeline/cloud/batch_layer/infrastructure/orchestration
./deploy_step_functions.sh
```

The script will:
1. Create/update the IAM role with required permissions
2. Create/update the Step Functions state machine
3. Create/update the EventBridge schedule rule

---

**Last Updated:** March 2026

