# Step Functions Pipeline Orchestration

The `dev-daily-ohlcv-pipeline` state machine orchestrates the daily OHLCV pipeline. It is created/updated by `deploy_step_functions.sh` and triggered Mon–Fri by an EventBridge Scheduler entry. Resource names below match what the deploy script provisions.

## Stages

```
Step Functions: dev-daily-ohlcv-pipeline
  STAGE 0  Plan              (Lambda, VPC)        reads watermark → fans out fetcher invokes
  STAGE 1  Fetch (parallel)  (Lambda, no VPC)     OHLCV + meta → S3 bronze
  STAGE 2  Ingest            (Lambda, VPC)        S3 → RDS upsert + watermark update
  STAGE 3  Build snapshot    (Lambda, VPC)        RDS 1d bars → scanner-snapshots/latest/market_1d.parquet
  STAGE 4  Vectorized scan   (Lambda, VPC)        whole-universe Polars scan → daily_scan_signals
           (CheckSnapshotFreshness fails fast if the snapshot has no bar for scan_date)
  STAGE 5  Aggregate         (Batch/Fargate)      global rank → stock_picks; clear staging

  ON FAILURE (any stage) → SNS: condvest-pipeline-alerts → Email
```

The vectorized scanner runs every strategy across ~12k symbols in one pass with Polars window functions, replacing the old partitioner + 10-child Fargate array worker. Total run ~18–22 min (dominated by a 15-min RDS-hydration wait).

## AWS resources

| Resource | Name | Stage |
|---|---|---|
| State machine | `dev-daily-ohlcv-pipeline` | — |
| EventBridge schedule | `dev-daily-ohlcv-pipeline-schedule` (cron `5 16 ? * MON-FRI *`, `America/New_York`) | — |
| SNS topic | `condvest-pipeline-alerts` | failure alerts |
| Planner | `dev-batch-daily-ohlcv-planner` | 0 |
| OHLCV / meta fetchers | `dev-batch-daily-ohlcv-fetcher`, `dev-batch-daily-meta-fetcher` | 1 |
| OHLCV / meta ingest | `dev-batch-daily-ohlcv-ingest-handler`, `dev-batch-daily-meta-ingest-handler` | 2 |
| Snapshot builder | `dev-batch-scanner-snapshot-builder` | 3 |
| Vectorized scanner | `dev-batch-vectorized-scanner` | 4 |
| Aggregator | `dev-batch-scanner-aggregator` (Batch/Fargate) | 5 |

Lambda stages retry twice with exponential backoff; the aggregator Batch stage retries twice. Exact settings live in `state_machine_definition.json`.

## Files

| File | Description |
|---|---|
| `state_machine_definition.json` | Step Functions ASL definition |
| `deploy_step_functions.sh` | Deploys state machine + IAM roles + SNS + EventBridge schedule |

## Deploy

```bash
cd cloud/batch_layer/infrastructure/orchestration
./deploy_step_functions.sh
```

## Trigger manually

Replace `<ACCOUNT_ID>` / `<REGION>` (default `ca-west-1`).

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:<REGION>:<ACCOUNT_ID>:stateMachine:dev-daily-ohlcv-pipeline" \
  --name "manual-$(date +%Y%m%d%H%M%S)" \
  --region <REGION>
```

Pin a specific OHLCV date when the execution date differs from the latest session in the snapshot (the scanner only emits signals for symbols with a bar on `scan_date`):

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:<REGION>:<ACCOUNT_ID>:stateMachine:dev-daily-ohlcv-pipeline" \
  --name "manual-scan-$(date +%Y%m%d%H%M%S)" \
  --region <REGION> \
  --input '{"scan_date":"2026-04-28"}'
```

If the snapshot has no bar for `scan_date` (`stale_snapshot: true`), `CheckSnapshotFreshness` routes to `NotifyFailure` instead of writing empty picks — re-run after ingest lands, or pin a date present in the snapshot.

## Disable / enable the schedule

The trigger uses **EventBridge Scheduler** (`aws scheduler …`), not classic rules:

```bash
aws scheduler update-schedule --name dev-daily-ohlcv-pipeline-schedule --state DISABLED --region <REGION>
aws scheduler update-schedule --name dev-daily-ohlcv-pipeline-schedule --state ENABLED  --region <REGION>
```
