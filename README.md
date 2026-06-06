# TradLyte — Backend Data Platform

Backend data pipeline for **TradLyte**, a trading-analytics platform. It ingests US-equities OHLCV from Polygon.io, persists raw + curated data in S3 and RDS, runs a daily full-universe strategy scanner at market close, and serves the results through a REST API consumed by the TradLyte frontend.

## Repository layout

| Path | What it does |
|---|---|
| `cloud/` | Production AWS stack (batch pipeline + serving API) |
| `cloud/batch_layer/` | Daily pipeline: fetch → ingest → snapshot → vectorized scan → aggregate |
| `cloud/serving_layer/` | FastAPI-on-Lambda REST API behind HTTP API Gateway |
| `cloud/shared/` | Cross-layer libraries: clients, models, utils, and the `analytics_core` strategy engine |
| `cloud/speed_layer/` | Archived real-time (Kinesis/Flink) design — not deployed |
| `local/` | Local Prefect dev stack (Bronze→Silver→Gold) for prototyping only |
| `docs/data_architecture.mmd` | Architecture diagram source |

## Pipeline at a glance

```
Polygon.io ─► Batch Layer (Step Functions, daily at market close) ─► stock_picks (RDS) ─► Serving API ─► Frontend
```

The batch layer fetches OHLCV + metadata to S3, ingests to RDS, builds a long-format market snapshot, runs every strategy across the whole universe in one vectorized pass, and ranks the results into `stock_picks`. The serving layer reads those tables over a REST API.

## Tech stack

AWS Lambda + AWS Batch (Fargate) + Step Functions + EventBridge · S3 + RDS PostgreSQL (+ RDS Proxy) · API Gateway HTTP API + FastAPI/Mangum · Polars / PyArrow + Pydantic v2 · Polygon.io. Deploys are bash + AWS CLI.

## Documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — orchestration flow + component reference
- [`cloud/README.md`](cloud/README.md) — cloud folder map and status
- [`cloud/serving_layer/API_GUIDE.md`](cloud/serving_layer/API_GUIDE.md) — HTTP API contract
- [`cloud/batch_layer/infrastructure/orchestration/README.md`](cloud/batch_layer/infrastructure/orchestration/README.md) — Step Functions operations

## License

Proprietary — see [`LICENSE`](LICENSE). Published for visibility only; not open source.
