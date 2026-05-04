# Serving layer

REST-facing APIs for frontend and consumers.

## MVP scope

The MVP serving API exposes six GET endpoints plus one POST backtest endpoint:

- `GET /v1/screener/quotes` - latest daily quote by filters (`industry`, `type`, `market cap` band).
- `GET /v1/picks/today` - ranked picks from latest `scan_date`.
- `GET /v1/picks/{scan_date}/returns` - pick performance (1d/5d/21d + return-to-date).
- `GET /v1/market/quote/{symbol}` - latest OHLCV quote for one symbol.
- `GET /v1/market/ohlcv/{symbol}` - OHLCV candle history by interval/date range.
- `GET /v1/market/returns/{symbol}` - 1d/5d/21d style returns for one symbol.
- `POST /v1/backtest` - single-symbol strategy backtest over date range.

All three are implemented in `lambda_functions/serving_api/` and deployed as a single VPC Lambda (`dev-serving-api`) behind HTTP API Gateway.

## Current status

- `GET /v1/health` is live and healthy.
- `GET /v1/screener/quotes` is live and healthy.
- `GET /v1/picks/today` is live and healthy.
- `GET /v1/picks/{scan_date}/returns` exists but may require additional query/index tuning for stable p95 latency.
- `GET /v1/market/quote/{symbol}` is available.
- `GET /v1/market/ohlcv/{symbol}` is available.
- `GET /v1/market/returns/{symbol}` is available.

## Code layout

| Path | Role |
| ------ | ------ |
| `lambda_functions/serving_api/` | FastAPI + Mangum serving Lambda (`dev-serving-api`) |
| `lambda_functions/backtester/backtest_handler.py` | POST backtest: RDS → Polars |
| `lambda_functions/backtester/Dockerfile` | Container image for backtester (ARM64) |
| `lambda_functions/backtester/requirements.backtester.txt` | Lean deps for that image |
| `infrastructure/serving_api/deploy_lambda.sh` | Package + deploy `dev-serving-api` |
| `infrastructure/serving_api/deploy_http_api.sh` | Create/update HTTP API routes + stage |
| `infrastructure/docker/build_push_backtester.sh` | Build & push backtester image to ECR |

## Serving API deploy sequence

From repository root:

```bash
./cloud/serving_layer/infrastructure/serving_api/deploy_lambda.sh
./cloud/serving_layer/infrastructure/serving_api/deploy_http_api.sh
```

Detailed variables and examples are in:
`cloud/serving_layer/infrastructure/serving_api/README.md`.

## Backtester container

From repository root:

```bash
AWS_REGION=ca-west-1 ECR_REPO=dev-serving-backtester \
  ./cloud/serving_layer/infrastructure/docker/build_push_backtester.sh
```

Lambda env vars:

- `RDS_SECRET_ARN` - Secrets Manager secret with `host`, `port`, `username`, `password`, `database` or `dbname`
- Optional: `BACKTEST_MAX_LOOKBACK_DAYS` (default `1825` ~= 5 years)

Use the **RDS Proxy** endpoint in the secret's `host` for production.

## Lessons learned (MVP rollout)

### 1) Proxy health can stall even when config appears correct

- **Issue:** RDS Proxy target stayed `UNAVAILABLE` (`PENDING_PROXY_CAPACITY`).
- **What we learned:** Target health diagnostics must be treated as first-class deployment gates.
- **Action taken:** Created a dedicated proxy SG and explicit DB ingress from proxy SG; validated target health before cutover.
- **Operational rule:** Never switch Lambda `RDS_SECRET_ARN` host to proxy until `describe-db-proxy-targets` reports `State=AVAILABLE`.

### 2) Lambda SG egress matters for proxy path

- **Issue:** Lambda timed out against proxy while health endpoint worked.
- **What we learned:** In this VPC, Lambda SG had restrictive egress and needed explicit 5432 egress to proxy SG.
- **Action taken:** Added Lambda SG egress rule to proxy SG on 5432.
- **Operational rule:** Keep a documented SG matrix: `Lambda SG -> Proxy SG (5432)`, `Proxy SG -> DB SG (5432)`, `DB SG inbound from Proxy SG (5432)`.

### 3) Secrets/IAM drift causes misleading runtime failures

- **Issue:** Data endpoints failed with `AccessDeniedException` from Secrets Manager while network looked healthy.
- **What we learned:** Secret ARN changes must be synchronized with role policies and Lambda env vars.
- **Action taken:** Aligned `RDS_SECRET_ARN` with role-allowed secret and refreshed Lambda configuration.
- **Operational rule:** Add a deployment check that verifies `GetSecretValue` permission for the configured secret ARN before traffic cutover.

### 4) API stage versioning and app route prefixes must be aligned

- **Issue:** API returned 404 because stage prefix and router prefixing were inconsistent.
- **What we learned:** Versioning should live in one layer (API stage/base path) and app routes should be stage-agnostic.
- **Action taken:** Normalized route keys and configured Mangum base path handling.
- **Operational rule:** Smoke test `/health` and one versioned data route immediately after API deploy.

### 5) Schema drift must be handled defensively in serving SQL

- **Issue:** `/picks/today` failed because `stock_picks.score` was absent in live DB schema.
- **What we learned:** Serving SQL should tolerate non-critical column drift in MVP.
- **Action taken:** Returned `NULL::numeric AS score` instead of hard-selecting a missing column.
- **Operational rule:** Add a pre-deploy schema compatibility check for serving queries.

### 6) Fetch/ingest decoupling is the right architecture

- **Issue (historical):** Monolithic fetch+write path increased blast radius and retry cost.
- **What we learned:** Separation of concerns is essential: stateless fetchers outside VPC, stateful ingest in VPC.
- **Action taken:** Kept fetchers no-VPC (external egress only), ingest/serving VPC-attached (private RDS path).
- **Operational rule:** Preserve this boundary; do not mix external API egress and private DB write/read in the same function unless strictly required.
