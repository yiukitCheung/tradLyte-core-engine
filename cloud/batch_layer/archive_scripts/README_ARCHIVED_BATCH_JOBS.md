# Archived Batch / Pipeline Scripts

Scripts in this folder were **removed from the active data pipeline** but kept for
reference and one-off re-runs. Each retirement is documented below.

> Note: archived deploy scripts still reference their *original* relative paths
> (e.g. `infrastructure/processing/lambda_functions/`). Move a copy back to its
> original location before running, or fix the path resolution at the top.

---

# 1) Resampler & Consolidator Batch Jobs

These components were **removed from the active data pipeline**. Resampling is now done **on-the-fly** in the backtester from raw 1d OHLCV.

## Archived files

| File | Original location | Purpose |
|------|-------------------|--------|
| `resampler.py` | `processing/batch_jobs/resampler.py` | DuckDB + S3 Fibonacci resampling (3d, 5d, 8d, …) to silver layer |
| `consolidator.py` | `processing/batch_jobs/consolidator.py` | Merge bronze `date=*.parquet` into `data.parquet` per symbol |
| `deploy_batch_jobs.sh` | `infrastructure/processing/deploy_batch_jobs.sh` | Deploy AWS Batch job definitions for consolidator/resampler |
| `build_batch_container.sh` | `infrastructure/processing/build_batch_container.sh` | Build/push Docker image for Batch jobs |

## Pipeline change

- **Before:** Step Function ran Fetchers → Consolidator (Batch) → Parallel Resamplers (Batch) → Complete.
- **After:** Step Function runs Fetchers → Complete. Multi-timeframe data for backtesting is produced by resampling 1d OHLCV at runtime (e.g. Polars `group_by_dynamic`).

## Re-enabling (if needed)

To run consolidator or resampler again (e.g. for a one-off backfill), use the scripts and job code in this archive. Deploy the state machine and Batch definitions from the archive copies; the Step Function definition was updated so it no longer invokes these steps.

---

# 2) Scanner Partitioner & Array-Job Worker Path

The per-symbol scanner was replaced by a **vectorized full-universe scanner**. A
dedicated Lambda (`dev-batch-scanner-snapshot-builder`) maintains a single
long-format Parquet snapshot (`scanner-snapshots/latest/market_1d.parquet`), and
a second Lambda (`dev-batch-vectorized-scanner`) scans the entire universe in one
pass with Polars window functions, writing to the same `daily_scan_signals`
staging table the aggregator reads. Offline parity vs. the per-symbol scanner was
validated (identical BUY sets and confidences).

## Archived files

| File | Original location | Purpose |
|------|-------------------|--------|
| `scan_partitioner.py` | `processing/lambda_functions/scan_partitioner.py` | Split active symbols into 10 `chunk_N.json` files on S3 for the Batch array job |
| `deploy_processing_lambda.sh` | `infrastructure/processing/lambda_functions/deploy_processing_lambda.sh` | Deploy the `dev-batch-scan-partitioner` Lambda |

## Pipeline change

- **Before:** `… → WaitForAsyncOHLCVIngest → BuildScannerSnapshot → PartitionSymbols (Lambda) → RunScannerWorkers (Batch Array Job × 10) → RunScannerAggregator`.
- **After:** `… → WaitForAsyncOHLCVIngest → BuildScannerSnapshot → RunVectorizedScanner (single Lambda) → CheckSnapshotFreshness → RunScannerAggregator`.

The `PartitionSymbols`, `MaybeRunScanner`, `PipelineSkippedScanner`, and
`RunScannerWorkers` states were removed from `state_machine_definition.json`.

## Still active (not archived)

- **`processing/batch_jobs/scan.py`** — its **aggregator** phase (`JOB_TYPE=scanner_aggregator`) still runs as `RunScannerAggregator`. The `run_worker` phase in the same file is now dead code, retained only to keep the aggregator entry point intact.
- The `dev-batch-scanner-aggregator` Batch job definition, queue, and container image remain in use.

## What still exists in AWS (no longer invoked)

These were left provisioned for easy rollback; they cost ~nothing while idle:

- Lambda `dev-batch-scan-partitioner`
- Batch job definition `dev-batch-scanner-worker`
- S3 prefix `scanner-chunks/{date}/` (no longer written)

## Re-enabling (if needed)

Restore the four removed states in `state_machine_definition.json` from git
history, move `scan_partitioner.py` + `deploy_processing_lambda.sh` back to their
original locations, redeploy the partitioner, and redeploy the state machine.
