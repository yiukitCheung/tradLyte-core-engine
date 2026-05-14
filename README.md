# TradLyte — Backend Data Platform

Backend data pipeline for **TradLyte**, a trading-analytics platform built around the principles of *"Clarity Over Noise"* and *"Purpose Over Profit"*. This repository contains the AWS-native production stack and a local development stack used for prototyping and replaying historical data.

The platform ingests US equities OHLCV from Polygon.io, persists raw + curated data, runs a daily strategy scanner against the full active universe, and serves the results through a REST API consumed by the TradLyte frontend.

---

## Repository layout

```
TradLyte-dp/
├── ARCHITECTURE.md            # Engineering reference (single source of truth)
├── LICENSE                    # Proprietary license
├── README.md                  # This file
├── cloud/                     # Production AWS implementation
│   ├── batch_layer/           # Daily ingest + scanner (Lambda + Batch + Step Functions)
│   ├── serving_layer/         # FastAPI on Lambda behind HTTP API Gateway
│   ├── speed_layer/           # Archived real-time design (Kinesis + Flink)
│   ├── shared/                # Cross-layer libs: clients, models, utils, analytics_core
│   ├── jupyter_notebook/      # Research notebooks driven against the cloud stack
│   ├── requirements.txt       # Pinned Python deps for cloud-side development
│   └── README.md              # Cloud architecture overview
├── local/                     # Local Prefect-based dev stack (Bronze → Silver → Gold)
│   ├── flows/, fetch/, process/, ingest/, database/, config/, ...
│   ├── prefect.yaml           # Prefect deployments
│   ├── Dockerfile             # Local worker image
│   └── requirements.txt
└── docs/
    └── data_architecture.mmd  # Authoritative Mermaid architecture diagram
```

---

## Solution overview

```
Polygon.io REST (OHLCV + Metadata)
              │
              ▼
   ┌──────────────────────────────────────────┐
   │            BATCH LAYER  (live)           │
   │                                          │
   │  EventBridge ─► Step Functions ─► …      │
   │     Plan ─► Fetch (S3 bronze) ─►         │
   │     Ingest (RDS) ─► Partition ─►         │
   │     Scanner Workers (×10 Fargate) ─►     │
   │     Aggregator ─► stock_picks            │
   └──────────────────────────────────────────┘
              │
              ▼
   ┌──────────────────────────────────────────┐
   │         SERVING LAYER  (MVP live)        │
   │                                          │
   │  HTTP API Gateway ─► dev-serving-api     │
   │  (FastAPI + Mangum) ─► RDS Proxy ─► RDS  │
   │  + dev-serving-backtester (planned       │
   │    container, code present, not deployed)│
   └──────────────────────────────────────────┘
```

The full diagram, with every Step Functions state, IAM boundary, network zone, and table, lives in [`ARCHITECTURE.md`](ARCHITECTURE.md) and [`docs/data_architecture.mmd`](docs/data_architecture.mmd).

---

## Technology stack

### Production (`cloud/`)

| Concern | Technology |
|---|---|
| Compute (event-driven) | AWS Lambda (Python 3.11, x86_64, zip packages) |
| Compute (batch) | AWS Batch on Fargate (containerised scanner) |
| Orchestration | AWS Step Functions (Standard) + EventBridge Scheduler |
| Object storage | Amazon S3 (Parquet for OHLCV, JSON manifests for metadata) |
| Relational store | Amazon RDS for PostgreSQL (private subnet) |
| Connection pooling | Amazon RDS Proxy (serving path) |
| Secrets | AWS Secrets Manager + Interface VPC Endpoint |
| Container registry | Amazon ECR (scanner + backtester images) |
| API surface | Amazon API Gateway (HTTP API) → FastAPI on Lambda via `Mangum` |
| Notifications | Amazon SNS (`condvest-pipeline-alerts`) |
| Logging | Amazon CloudWatch Logs |
| Market-data provider | Polygon.io REST (`polygon-api-client`) |
| Data processing | `polars`, `pyarrow`, `pandas` |
| Data validation | `pydantic` v2 (DTOs at every boundary) |
| Database driver | `psycopg2-binary` |
| API framework | `fastapi`, `mangum`, `uvicorn` |
| IaC tooling | Bash + AWS CLI v2 (no Terraform / CDK today) |

Pinned versions live in [`cloud/requirements.txt`](cloud/requirements.txt). Function-specific deps are vendored per Lambda by the deploy scripts (`pip_for_lambda.sh` builds manylinux wheels for native packages such as `psycopg2` and `pyarrow`).

### Local development (`local/`)

| Concern | Technology |
|---|---|
| Orchestration | Prefect 3.x (`prefect.yaml` defines silver/gold deployments) |
| Storage | Local PostgreSQL (Docker), DuckDB, Parquet on disk |
| Worker runtime | Docker (`local/Dockerfile`) |
| Data processing | `polars`, `duckdb`, `pyarrow`, `pandas` |
| Market calendar | `exchange_calendars`, `pandas_market_calendars` |
| Realtime / streaming | `websockets`, `kafka-python` (used in archived speed-layer code) |
| Market-data provider | Polygon.io REST + `yfinance` for backfills |

The local stack is **not** part of the production path; it exists for rapid iteration on indicator/strategy code and for offline replay of historical datasets. Pinned versions live in [`local/requirements.txt`](local/requirements.txt).

### Cross-cutting libraries (`cloud/shared/`)

- `shared.clients.rds_timescale_client` — RDS connection (Secrets Manager + VPC endpoint), OHLCV upsert, watermark helpers.
- `shared.clients.polygon_client` — Sync + async Polygon REST wrapper.
- `shared.models.data_models` — Pydantic DTOs (`OHLCVData`, `BatchProcessingJob`, …).
- `shared.utils.pipeline` — Watermark + 5-year rolling retention helpers.
- `shared.utils.market_calendar` — US/Eastern trading-day arithmetic.
- `shared.analytics_core` — Strategy framework: `indicators/`, `strategies/{base,builder,library}`, `scanner.py`, `backtester.py`, `executor.py`, `inputs.py`.

---

## Project status

> **Environment:** all AWS resources currently live under the `dev-` prefix (region `ca-west-1`). `stg-` / `prod-` environments are not yet provisioned.

| Component | Status | Notes |
|---|---|---|
| Batch Layer — fetchers (`daily-ohlcv-fetcher`, `daily-meta-fetcher`) | Deployed | Stateless Lambda, no VPC, public egress to Polygon |
| Batch Layer — planner (`daily-ohlcv-planner`) | Deployed | VPC Lambda, reads watermark, fans out fetch invokes |
| Batch Layer — ingest handlers (OHLCV, Meta) | Deployed | VPC Lambda, S3 → RDS upsert, supports SQS path |
| Batch Layer — scanner partitioner | Deployed | Lambda; writes 10 chunks to S3 |
| Batch Layer — scanner worker (×10) + aggregator | Deployed | AWS Batch on Fargate, Array Job |
| Step Functions pipeline (`dev-daily-ohlcv-pipeline`) | Deployed | Triggered Mon–Fri 4:05 PM America/New_York |
| SNS failure alerts (`condvest-pipeline-alerts`) | Deployed | Subscribe an email to receive notifications |
| Serving API (`dev-serving-api`) | Live in dev | `/v1/health`, `/v1/screener/*`, `/v1/picks/*`, `/v1/market/*` |
| HTTP API Gateway (`dev-serving-http-api`) + RDS Proxy (`dev-rds-proxy-v2`) | Deployed | Stage `v1`, CORS + per-route throttling |
| Backtester Lambda (`dev-serving-backtester`) | Code + Dockerfile present, **not deployed** | Container image (ARM64), invoked by serving API when present |
| ElastiCache Redis | **Deferred** | Not required at current read traffic; in-process LRU only |
| Speed Layer (Kinesis + Flink) | **Archived** under `cloud/speed_layer/Archive/` | Parked until a real-time product requirement appears |
| Local Prefect medallion stack (`local/`) | Operational | For prototyping only; not used in production path |
| Resampler / consolidator Batch jobs | **Removed** | Resampling is now done on the fly in the backtester (`shared.analytics_core.inputs.build_multi_timeframe_from_batch_1d`); archived under `cloud/batch_layer/archive_scripts/` |

**Current cost envelope (dev account):** ~$69 / month (Batch ~$59 + Serving ~$10). Activating the archived Speed Layer would add ~$110 / month.

**Active focus:**
1. Backtester Lambda deployment + end-to-end smoke test of `POST /v1/backtest`.
2. Latency tuning for `GET /v1/picks/{scan_date}/returns`.
3. Hardening for promotion to `stg-` / `prod-` (secrets rotation, IaC migration, CloudWatch dashboards).

---

## Documentation map

| Document | Scope |
|---|---|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Engineering reference: components, data, security, observability, risks. |
| [`docs/data_architecture.mmd`](docs/data_architecture.mmd) | Authoritative Mermaid diagram of the full pipeline. |
| [`cloud/README.md`](cloud/README.md) | Cloud-side architecture overview + cost summary. |
| [`cloud/serving_layer/README.md`](cloud/serving_layer/README.md) | Serving layer scope + rollout lessons learned. |
| [`cloud/serving_layer/API_GUIDE.md`](cloud/serving_layer/API_GUIDE.md) | HTTP API contract: routes, auth, payload examples. |
| [`cloud/serving_layer/infrastructure/serving_api/README.md`](cloud/serving_layer/infrastructure/serving_api/README.md) | Serving Lambda + HTTP API + RDS Proxy deploy runbook. |
| [`cloud/batch_layer/infrastructure/orchestration/README.md`](cloud/batch_layer/infrastructure/orchestration/README.md) | Step Functions state machine + EventBridge schedule. |
| [`cloud/batch_layer/database/schemas/migrations/README.md`](cloud/batch_layer/database/schemas/migrations/README.md) | Local Postgres → RDS migration tooling. |
| [`cloud/batch_layer/archive_scripts/README_ARCHIVED_BATCH_JOBS.md`](cloud/batch_layer/archive_scripts/README_ARCHIVED_BATCH_JOBS.md) | Why consolidator/resampler were retired and how to revive them. |
| [`cloud/speed_layer/Archive/infrastructure/SPEED_LAYER_REQUIREMENTS.md`](cloud/speed_layer/Archive/infrastructure/SPEED_LAYER_REQUIREMENTS.md) | Original Kinesis/Flink design doc (archived). |

---

## License & usage

This repository is published for visibility only. It is **private intellectual property** and is **not open source**.

- You may view the code on GitHub.
- You may **not** copy, modify, distribute, sublicense, sell, or use this code for commercial or non-commercial purposes without explicit written permission from the owner.
- All rights are reserved by the owner.

See [`LICENSE`](LICENSE) for the full legal terms.

---

## Related projects

- **TradLyte Frontend** — React frontend, consumes the serving API in this repo. (Repository link withheld pending re-verification.)

---

**Maintained by:** TradLyte Platform Team
**Last updated:** May 2026
