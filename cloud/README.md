# TradLyte Cloud Data Pipeline

## Overview

This directory contains the AWS-native implementation of the TradLyte data pipeline using a **Lambda Architecture** pattern (Batch + Serving). The Speed Layer (Kinesis / Flink) was designed but parked for the MVP — its code lives under `speed_layer/Archive/`.

The MVP follows the project principle of *"Clarity Over Noise"*:

- No real-time streaming in the MVP — keeps signal quality high and cost bounded.
- Strict separation of **fetch (stateless, no VPC)** from **ingest (VPC, stateful)** so external egress and private DB access never share blast radius.

For the full engineering reference, see [`../ARCHITECTURE.md`](../ARCHITECTURE.md). The authoritative diagram lives at [`../docs/data_architecture.mmd`](../docs/data_architecture.mmd).

---

## Architecture diagram (high level)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           TRADLYTE CLOUD PIPELINE                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────────────────────────────────────────────────────────────┐   │
│   │                            DATA SOURCE                                │   │
│   │            Polygon.io REST  (OHLCV + Symbol metadata)                 │   │
│   └────────────────────────────────┬─────────────────────────────────────┘   │
│                                    │                                         │
│   ┌────────────────────────────────▼─────────────────────────────────────┐   │
│   │                          BATCH LAYER (live)                           │   │
│   │                                                                       │   │
│   │  EventBridge Scheduler ─► Step Functions: dev-daily-ohlcv-pipeline    │   │
│   │                                                                       │   │
│   │   STAGE 0  Plan (VPC Lambda)                                          │   │
│   │      reads watermark, fans out per-date fetcher invokes               │   │
│   │                                                                       │   │
│   │   STAGE 1  Parallel fetchers (Lambda, NO VPC)                         │   │
│   │      OHLCV Fetcher  →  s3://…/bronze/raw_ohlcv/                       │   │
│   │      Meta  Fetcher  →  s3://…/bronze/raw_meta/   (+ _manifest.json)   │   │
│   │                                                                       │   │
│   │   STAGE 2  Ingest handlers (VPC Lambda)                               │   │
│   │      OHLCV: parquet → RDS upsert (+ SCD-2 watermark update)           │   │
│   │      Meta : manifest → symbol_metadata upsert                         │   │
│   │                                                                       │   │
│   │   STAGE 3  Partition (VPC Lambda)                                     │   │
│   │      reads symbol_metadata once → writes 10 chunk_N.json to S3        │   │
│   │                                                                       │   │
│   │   STAGE 4  Scanner Workers  (AWS Batch / Fargate, Array Job × 10)     │   │
│   │      run strategies → daily_scan_signals (RDS staging)                │   │
│   │                                                                       │   │
│   │   STAGE 5  Scanner Aggregator (AWS Batch / Fargate, single job)       │   │
│   │      global rank → stock_picks; truncate staging                      │   │
│   │                                                                       │   │
│   │   ON FAILURE (any stage) → SNS: condvest-pipeline-alerts → Email      │   │
│   └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│   ┌────────────────────────────────▼─────────────────────────────────────┐   │
│   │                       SERVING LAYER (MVP live)                        │   │
│   │                                                                       │   │
│   │   Frontend (HTTPS + x-api-key)                                        │   │
│   │       │                                                               │   │
│   │       ▼                                                               │   │
│   │   API Gateway (HTTP API)                                              │   │
│   │       │                                                               │   │
│   │       ▼                                                               │   │
│   │   dev-serving-api  (FastAPI + Mangum, Lambda, VPC)                    │   │
│   │       │                                                               │   │
│   │       ▼                                                               │   │
│   │   RDS Proxy ─► RDS PostgreSQL                                         │   │
│   │                                                                       │   │
│   │   POST /v1/backtest  ─► dev-serving-backtester  (container, planned)  │   │
│   └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory structure

```
cloud/
├── README.md                                # This file
├── requirements.txt                         # Pinned deps for cloud-side dev
│
├── batch_layer/                             # Daily ingest + scanner pipeline
│   ├── archive_scripts/                     # Retired consolidator/resampler jobs
│   │   └── README_ARCHIVED_BATCH_JOBS.md
│   ├── command/                             # Ad-hoc CLI helpers
│   ├── database/
│   │   ├── lambda_functions/                # DB-bootstrap Lambda (init/migrations)
│   │   ├── migrations/
│   │   └── schemas/
│   │       ├── schema_init.sql
│   │       ├── retention_policy.sql
│   │       ├── functions.sql
│   │       ├── timescale_schema_init.sql    # Vestigial (Timescale variant)
│   │       └── migrations/                  # Python migration runners + README
│   │           ├── README.md
│   │           ├── migrate.py
│   │           └── migrate_fast.py
│   ├── deploy.sh                            # Top-level batch-layer convenience script
│   ├── fetching/
│   │   └── lambda_functions/
│   │       ├── daily_ohlcv_fetcher.py       # Stateless OHLCV fetcher (no VPC)
│   │       ├── daily_ohlcv_planner.py       # VPC planner (reads watermark, fans out)
│   │       └── daily_meta_fetcher.py        # Stateless metadata fetcher (no VPC)
│   ├── ingesting/
│   │   ├── requirements.txt
│   │   └── lambda_functions/
│   │       ├── daily_ohlcv_ingest_handler.py # VPC: parquet → RDS upsert
│   │       └── daily_meta_ingest_handler.py  # VPC: manifest → symbol_metadata upsert
│   ├── infrastructure/
│   │   ├── common/
│   │   │   ├── VPC_LAMBDA_SECRETS_MANAGER.txt
│   │   │   ├── create_secretsmanager_vpc_endpoint.sh
│   │   │   └── pip_for_lambda.sh
│   │   ├── fetching/                        # deploy_lambda.sh for fetcher + planner
│   │   ├── ingesting/                       # deploy_lambda.sh for ingest handlers
│   │   ├── orchestration/
│   │   │   ├── README.md
│   │   │   ├── state_machine_definition.json
│   │   │   └── deploy_step_functions.sh
│   │   └── processing/
│   │       ├── batch_job/                   # Scanner image + Batch job definitions
│   │       │   ├── Dockerfile               # (vestigial, was for resampler)
│   │       │   ├── Dockerfile.scanner
│   │       │   ├── build_scanner_container.sh
│   │       │   ├── deploy_scanner_batch_jobs.sh
│   │       │   └── wire_scanner_to_rds_proxy.sh
│   │       └── lambda_functions/
│   │           └── deploy_processing_lambda.sh   # Deploys scan_partitioner Lambda
│   └── processing/
│       ├── batch_jobs/
│       │   ├── scan.py                      # Scanner worker + aggregator entry point
│       │   ├── requirements.scanner.txt     # Lean scanner deps
│       │   └── requirements.txt             # Full deps
│       └── lambda_functions/
│           └── scan_partitioner.py          # Symbols → S3 chunks
│
├── serving_layer/                           # API serving (MVP live)
│   ├── README.md
│   ├── API_GUIDE.md                         # HTTP API contract
│   ├── lambda_functions/
│   │   ├── requirements.txt
│   │   ├── serving_api/                     # FastAPI app (dev-serving-api)
│   │   │   ├── app.py, handler.py, db.py, cache.py, models.py
│   │   │   └── routers/                     # screener, picks, market, backtest
│   │   └── backtester/                      # Container Lambda (dev-serving-backtester, planned)
│   │       ├── backtest_handler.py
│   │       ├── Dockerfile
│   │       └── requirements.backtester.txt
│   └── infrastructure/
│       ├── serving_api/
│       │   ├── README.md
│       │   ├── deploy_lambda.sh
│       │   └── deploy_http_api.sh
│       ├── backtester/
│       │   └── build_push_backtester.sh
│       └── docker/                          # Duplicate of backtester/ (pending consolidation)
│           └── build_push_backtester.sh
│
├── speed_layer/                             # Archived Kinesis/Flink design
│   ├── websocket_connect.py
│   ├── websocket_disconnect.py
│   └── Archive/
│       ├── fetching/                        # ECS data-stream fetcher
│       ├── infrastructure/                  # Task defs, build scripts, requirements doc
│       ├── kinesis_analytics/               # Flink SQL resampler apps
│       ├── lambda_functions/                # Kinesis → DynamoDB handlers
│       └── shared/
│
├── shared/                                  # Used by batch + serving
│   ├── __init__.py
│   ├── clients/
│   │   ├── polygon_client.py                # Polygon REST wrapper
│   │   └── rds_timescale_client.py          # RDS connection + upsert helpers
│   ├── models/
│   │   └── data_models.py                   # Pydantic DTOs
│   ├── utils/
│   │   ├── market_calendar.py               # US/Eastern trading-day arithmetic
│   │   └── pipeline.py                      # Watermark + 5-yr retention helpers
│   └── analytics_core/                      # Strategy framework
│       ├── indicators/                      # technicals, patterns (Polars-native)
│       ├── strategies/                      # base, builder, library
│       ├── scanner.py                       # DailyScanner.run → rank → write
│       ├── backtester.py
│       ├── executor.py
│       ├── inputs.py                        # OHLCV loaders + on-the-fly resampling
│       └── models.py
│
└── jupyter_notebook/                        # Research notebooks
    ├── batch_layer_analytics_engine.ipynb
    ├── batch_layer_data_fetch.ipynb
    └── batch_layer_processing.ipynb
```

---

## Implementation status

### Batch Layer (live in dev)

| Component | AWS resource | Status |
|---|---|---|
| OHLCV planner Lambda | `dev-batch-daily-ohlcv-planner` | Deployed |
| OHLCV fetcher Lambda | `dev-batch-daily-ohlcv-fetcher` | Deployed |
| Metadata fetcher Lambda | `dev-batch-daily-meta-fetcher` | Deployed |
| OHLCV ingest handler (Lambda) | `dev-batch-daily-ohlcv-ingest-handler` | Deployed |
| Metadata ingest handler (Lambda) | `dev-batch-daily-meta-ingest-handler` | Deployed |
| Scanner partitioner (Lambda) | `dev-batch-scan-partitioner` | Deployed |
| Scanner workers (AWS Batch on Fargate, Array × 10) | `dev-batch-scanner-worker` | Deployed |
| Scanner aggregator (AWS Batch on Fargate) | `dev-batch-scanner-aggregator` | Deployed |
| Step Functions state machine | `dev-daily-ohlcv-pipeline` | Deployed |
| EventBridge schedule | `dev-daily-ohlcv-pipeline-schedule` | Mon–Fri 4:05 PM America/New_York |
| SNS failure topic | `condvest-pipeline-alerts` | Configured |
| Watermark table (SCD Type 2) | `data_ingestion_watermark` | In schema |
| Scanner staging table | `daily_scan_signals` | In schema |
| Scanner output table | `stock_picks` | In schema |
| Resampling | On the fly | Backtester resamples 1d → Fibonacci intervals at query time (`shared.analytics_core.inputs.build_multi_timeframe_from_batch_1d`) |

### Serving Layer (MVP live in dev)

| Component | AWS resource | Status |
|---|---|---|
| Serving API Lambda | `dev-serving-api` (FastAPI + Mangum, VPC, zip) | Deployed |
| HTTP API Gateway | `dev-serving-http-api`, stage `v1` | Deployed |
| RDS Proxy | `dev-rds-proxy-v2` | Deployed |
| Backtester Lambda | `dev-serving-backtester` (container, ARM64) | Code + Dockerfile present, **not deployed** |

Live routes (subject to API key when `SERVING_API_KEY[_SECRET_ARN]` is set on the Lambda):

| Route | Description |
|---|---|
| `GET /v1/health` | Liveness probe (no API key) |
| `GET /v1/screener/quotes` | Filtered universe with latest daily OHLCV |
| `GET /v1/picks/today` | Latest `scan_date` ranked picks |
| `GET /v1/picks/today/metadata` | Same with `metadata` JSON column |
| `GET /v1/picks/detail` | One symbol + scan date joined with metadata |
| `GET /v1/picks/{scan_date}/returns` | Per-pick return horizons (1d / 5d / 21d, configurable) |
| `GET /v1/market/quote/{symbol}` | Latest daily bar + metadata |
| `GET /v1/market/news/{symbol}` | Polygon news feed |
| `GET /v1/market/ohlcv/{symbol}` | OHLCV history by interval |
| `GET /v1/market/returns/{symbol}` | Multi-horizon returns from daily closes |
| `POST /v1/backtest` | Single-symbol strategy backtest (proxied to backtester Lambda) |

### Speed Layer (archived)

A Kinesis Data Streams + Kinesis Analytics (Flink SQL) + DynamoDB pipeline was designed (ECS WebSocket fetcher, Flink resampler apps, Lambda → DynamoDB sink). Parked for MVP — code preserved under [`speed_layer/Archive/`](speed_layer/Archive/) and design doc at [`speed_layer/Archive/infrastructure/SPEED_LAYER_REQUIREMENTS.md`](speed_layer/Archive/infrastructure/SPEED_LAYER_REQUIREMENTS.md).

### Local dev stack (`../local/`)

A separate Prefect-based Bronze→Silver→Gold pipeline lives at [`../local/`](../local/) and is intended for prototyping only — it is not part of the production path.

---

## Daily flow

```
Market Close (4:00 PM America/New_York)
         │
         ▼  4:05 PM America/New_York  (EventBridge Scheduler)
   Step Functions: dev-daily-ohlcv-pipeline
         │
         ▼  STAGE 0 — Plan (VPC Lambda)
   Reads data_ingestion_watermark → get_missing_dates()
   Fans out fetcher invokes (async per-date or sync single-date)
         │
         ▼  STAGE 1 — Parallel Fetchers (Lambda, NO VPC)         ~ 3 min
   OHLCV  → S3 bronze (parquet)
   Meta   → S3 bronze (JSON parts + manifest)
         │
         ▼  STAGE 2 — Ingest Handlers (VPC Lambda)               ~ 1–2 min
   OHLCV  → RDS upsert + SCD-2 watermark update
   Meta   → symbol_metadata upsert
         │
         ▼  STAGE 3 — Partition Symbols (VPC Lambda)             ~ 30 sec
   1 RDS query + 10 chunk_N.json files to S3
         │
         ▼  STAGE 4 — Scanner Workers (Fargate Array × 10)       ~ 10–20 min
   Each container: download chunk → load OHLCV → run strategies
   → daily_scan_signals (RDS staging)
         │
         ▼  STAGE 5 — Scanner Aggregator (Fargate, single)       ~ 1–2 min
   Global rank → stock_picks → truncate staging
         │
         ▼
   ✅ Pipeline complete   (~15–25 min total)

   ON FAILURE (any stage) → SNS condvest-pipeline-alerts → Email
```

---

## Cost envelope (dev account, current)

| Service | Monthly cost |
|---|---|
| **Batch Layer** | |
| Lambda (planner + fetchers + ingest + partitioner) | $5 |
| RDS (t3.micro) | $20 |
| S3 storage | $10 |
| AWS Batch (scanner Array Job + aggregator) | $20 |
| ECR (scanner image) | $1 |
| Step Functions | $2 |
| SNS alerts | $1 |
| **Batch Layer subtotal** | **~$59** |
| | |
| **Serving Layer** | |
| Serving API (Lambda + HTTP API Gateway + RDS Proxy) | ~$10 |
| Backtester (Lambda, container — *not yet deployed*) | +$3 when live |
| **Serving Layer subtotal** | **~$10 today / ~$13 with backtester** |
| | |
| **Total MVP (dev)** | **~$69 / month** |

Activating the archived Speed Layer would add ~$110 / month; deferred until real-time signals become a product requirement.

---

## Documentation

- [Engineering reference](../ARCHITECTURE.md)
- [Architecture diagram](../docs/data_architecture.mmd)
- [Serving Layer overview](serving_layer/README.md)
- [Serving API HTTP guide](serving_layer/API_GUIDE.md)
- [Serving API deploy runbook](serving_layer/infrastructure/serving_api/README.md)
- [Step Functions orchestration guide](batch_layer/infrastructure/orchestration/README.md)
- [Database migration tooling](batch_layer/database/schemas/migrations/README.md)
- [Archived batch jobs (resampler / consolidator)](batch_layer/archive_scripts/README_ARCHIVED_BATCH_JOBS.md)
- [Archived speed-layer design](speed_layer/Archive/infrastructure/SPEED_LAYER_REQUIREMENTS.md)

---

## Key design choices

1. **Serverless-first**: pay only for what you use; no always-on servers in the batch path.
2. **Fetch / ingest / plan decoupling**: stateless fetchers outside the VPC (cheap egress to Polygon), stateful ingest inside the VPC (private RDS access). S3 is the source of truth, ingest is replayable.
3. **Idempotent upserts**: `ON CONFLICT DO UPDATE` on all writes; replays are safe.
4. **Step Functions over cron-and-pray**: visual execution graph, per-state retries, automatic failure SNS.
5. **Resampling at read time**: the silver tables in the schema are vestigial; the backtester computes Fibonacci intervals from `raw_ohlcv` on demand using Polars.
6. **API gateway in front of FastAPI**: keeps routing/throttling/CORS in API Gateway and lets us reuse the same FastAPI app locally.
7. **MVP-aligned**: no real-time streaming yet — *"Clarity Over Noise"* over architectural ambition.

---

**Last updated:** May 2026
**Overall status:** Batch Layer live · Serving Layer MVP live (screener + picks + market) · Backtester pending deploy · Speed Layer archived
