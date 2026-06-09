# TradLyte Cloud Data Pipeline

AWS-native implementation of the TradLyte platform: a daily **Batch Layer** (ingest + scanner) and a live **Serving Layer** (REST API). Retired code lives under `_archive/`.

For the orchestration flow and component reference, see [`../ARCHITECTURE.md`](../ARCHITECTURE.md).

## Directory structure

```
cloud/
├── batch_layer/                             # Daily ingest + scanner pipeline
│   ├── database/                            # Schemas, migrations, DB-bootstrap Lambda
│   ├── fetching/lambda_functions/           # OHLCV/meta fetchers (no VPC) + planner (VPC)
│   ├── ingesting/lambda_functions/          # S3 → RDS upsert handlers (VPC)
│   ├── processing/
│   │   ├── batch_jobs/aggregator.py         # Scanner aggregator (rank → stock_picks)
│   │   └── lambda_functions/
│   │       ├── snapshot_builder.py          # RDS → long-format market_1d.parquet snapshot
│   │       └── scanner.py                   # Full-universe scan → daily_scan_signals
│   └── infrastructure/                      # Deploy scripts (fetching, ingesting, processing, orchestration)
│
├── serving_layer/                           # REST API (live in dev)
│   ├── API_GUIDE.md                         # HTTP API contract
│   ├── lambda_functions/
│   │   ├── serving_api/                     # FastAPI app (dev-serving-api) + routers
│   │   └── backtester/                      # Container Lambda (dev-serving-backtester)
│   └── infrastructure/                      # Serving API + backtester deploy scripts
│
├── _archive/                                # Retired batch + speed-layer code (not deployed)
│
├── shared/                                  # Used by batch + serving
│   ├── clients/                             # Polygon REST + RDS client
│   ├── models/                              # Pydantic DTOs
│   ├── utils/                               # Market calendar + watermark/retention helpers
│   └── analytics_core/                      # Strategy engine (indicators, strategies, scanner, backtester)
│
└── jupyter_notebook/                        # Research notebooks
```

## Status

### Batch Layer (live in dev)

| Component | AWS resource |
|---|---|
| Planner / OHLCV fetcher / meta fetcher | `dev-batch-daily-ohlcv-planner`, `-ohlcv-fetcher`, `-meta-fetcher` |
| OHLCV / meta ingest handlers | `dev-batch-daily-ohlcv-ingest-handler`, `-meta-ingest-handler` |
| Snapshot builder | `dev-batch-scanner-snapshot-builder` |
| Scanner | `dev-batch-scanner` |
| Aggregator | `dev-batch-scanner-aggregator` (Batch/Fargate) |
| Orchestrator + schedule | `dev-daily-ohlcv-pipeline` (Step Functions), Mon–Fri 4:05 PM ET |

Multi-timeframe bars are resampled on the fly from 1d.

### Serving Layer (live in dev)

| Component | AWS resource |
|---|---|
| Serving API | `dev-serving-api` (FastAPI + Mangum, VPC) behind `dev-serving-http-api` |
| Backtester | `dev-serving-backtester` (container, ARM64) |
| RDS Proxy | `dev-rds-proxy-v2` |

Live routes: `GET /v1/health`, `/v1/screener/quotes`, `/v1/picks/*`, `/v1/market/*`, and `POST /v1/backtest`. Full contract in [`serving_layer/API_GUIDE.md`](serving_layer/API_GUIDE.md).

### Speed Layer (archived)

A Kinesis + Flink + DynamoDB real-time design, parked for the MVP. Code under [`_archive/speed_layer/`](_archive/speed_layer/).

## Documentation

- [Architecture reference](../ARCHITECTURE.md)
- [Serving API contract](serving_layer/API_GUIDE.md)
- [Step Functions operations](batch_layer/infrastructure/orchestration/README.md)
- [Database migration tooling](batch_layer/database/migrations/README_data_migration.md)
- [Security groups (consolidated)](docs/operations/SECURITY_GROUPS.md)
- [Minimum AWS stack checklist](docs/operations/AWS_MINIMUM_STACK.md)
