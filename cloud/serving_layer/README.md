# Serving layer

REST-facing APIs for the TradLyte frontend and other consumers. The full HTTP contract (routes, query parameters, payload examples, auth model, CORS) lives in [`API_GUIDE.md`](API_GUIDE.md).

## MVP scope

The MVP serving API exposes ten GET routes and one POST route, all behind API Gateway HTTP API stage `v1`:

- `GET /v1/health` — liveness probe (no API key).
- `GET /v1/screener/quotes` — latest daily quote by filters (industry, type, market-cap band).
- `GET /v1/picks/today` — ranked picks from the latest `scan_date`.
- `GET /v1/picks/today/metadata` — same with the `metadata` JSON column.
- `GET /v1/picks/detail` — one symbol + scan date, joined with `symbol_metadata`.
- `GET /v1/picks/{scan_date}/returns` — pick performance (configurable horizons, default 1d / 5d / 21d).
- `GET /v1/market/quote/{symbol}` — latest OHLCV quote for one symbol.
- `GET /v1/market/news/{symbol}` — Polygon news feed for a symbol.
- `GET /v1/market/ohlcv/{symbol}` — OHLCV candle history by interval / date range.
- `GET /v1/market/returns/{symbol}` — multi-horizon returns from daily closes.
- `POST /v1/backtest` — single-symbol strategy backtest over a date range.

`GET` routes and `POST /v1/backtest` are implemented in [`lambda_functions/serving_api/`](lambda_functions/serving_api/) and deployed as a single VPC Lambda (`dev-serving-api`, FastAPI + Mangum) behind an HTTP API Gateway. `POST /v1/backtest` proxies execution to a separate container Lambda (`dev-serving-backtester`) so heavy analytics dependencies stay out of the main serving zip.

## Current status

| Route | Status |
|---|---|
| `GET /v1/health` | Live |
| `GET /v1/screener/quotes` | Live |
| `GET /v1/picks/today` | Live |
| `GET /v1/picks/today/metadata` | Live |
| `GET /v1/picks/detail` | Live |
| `GET /v1/picks/{scan_date}/returns` | Live (latency tuning in progress for cold paths) |
| `GET /v1/market/quote/{symbol}` | Live |
| `GET /v1/market/news/{symbol}` | Live |
| `GET /v1/market/ohlcv/{symbol}` | Live |
| `GET /v1/market/returns/{symbol}` | Live |
| `POST /v1/backtest` | Code present, **backtester Lambda not deployed yet** — the route returns an error until `dev-serving-backtester` is built and registered |

## Code layout

| Path | Role |
|---|---|
| `lambda_functions/serving_api/` | FastAPI + Mangum serving Lambda (`dev-serving-api`) |
| `lambda_functions/serving_api/routers/` | Route handlers (`screener`, `picks`, `market`, `backtest`) |
| `lambda_functions/serving_api/db.py`, `cache.py`, `models.py` | Shared serving plumbing (RDS, in-process LRU, response models) |
| `lambda_functions/backtester/backtest_handler.py` | Container Lambda entry point |
| `lambda_functions/backtester/Dockerfile` | Container image for backtester (ARM64) |
| `lambda_functions/backtester/requirements.backtester.txt` | Lean deps for the backtester image |
| `infrastructure/serving_api/deploy_lambda.sh` | Package + deploy `dev-serving-api` |
| `infrastructure/serving_api/deploy_http_api.sh` | Create / update HTTP API routes + `v1` stage |
| `infrastructure/backtester/build_push_backtester.sh` | Build & push backtester image to ECR |
| `infrastructure/docker/build_push_backtester.sh` | Older copy of the same script (kept temporarily; consolidate before promoting to prod) |

## Serving API deploy sequence

From the repository root:

```bash
./cloud/serving_layer/infrastructure/serving_api/deploy_lambda.sh
./cloud/serving_layer/infrastructure/serving_api/deploy_http_api.sh
```

Detailed environment variables, console-side RDS Proxy creation, and the SG matrix are documented in [`infrastructure/serving_api/README.md`](infrastructure/serving_api/README.md).

## Backtester container

From the repository root:

```bash
AWS_REGION=ca-west-1 ECR_REPO=dev-serving-backtester \
  ./cloud/serving_layer/infrastructure/backtester/build_push_backtester.sh
```

After the image is in ECR, create the `dev-serving-backtester` Lambda from that image and grant `dev-serving-api` permission to invoke it (`BACKTEST_FUNCTION_NAME=dev-serving-backtester`).

Lambda env vars (backtester):

- `RDS_SECRET_ARN` — Secrets Manager secret with `host`, `port`, `username`, `password`, and `database`/`dbname` keys.
- `BACKTEST_MAX_LOOKBACK_DAYS` — optional, default `1825` (~5 years).

Use the **RDS Proxy** endpoint in the secret's `host` for production traffic.

## Lessons learned (MVP rollout)

### 1) Proxy health can stall even when config appears correct

- **Issue:** RDS Proxy target stayed `UNAVAILABLE` (`PENDING_PROXY_CAPACITY`).
- **What we learned:** target health diagnostics must be treated as first-class deployment gates.
- **Action taken:** dedicated proxy SG and explicit DB ingress from proxy SG; validated target health before cutover.
- **Operational rule:** never switch the Lambda `RDS_SECRET_ARN` host to the proxy until `describe-db-proxy-targets` reports `State=AVAILABLE`.

### 2) Lambda SG egress matters for the proxy path

- **Issue:** Lambda timed out against the proxy while the health endpoint worked.
- **What we learned:** in this VPC, the Lambda SG had restrictive egress and needed an explicit 5432 egress rule to the proxy SG.
- **Action taken:** added Lambda SG egress to proxy SG on 5432.
- **Operational rule:** keep a documented SG matrix — `Lambda SG → Proxy SG (5432)`, `Proxy SG → DB SG (5432)`, `DB SG inbound from Proxy SG (5432)`.

### 3) Secrets / IAM drift causes misleading runtime failures

- **Issue:** data endpoints failed with `AccessDeniedException` from Secrets Manager while the network looked healthy.
- **What we learned:** secret ARN changes must be synchronised with role policies and Lambda env vars.
- **Action taken:** aligned `RDS_SECRET_ARN` with the role-allowed secret and refreshed Lambda configuration.
- **Operational rule:** add a deployment check that verifies `GetSecretValue` permission for the configured secret ARN before cutover.

### 4) API stage versioning and app route prefixes must be aligned

- **Issue:** API returned 404 because the stage prefix and router prefixing were inconsistent.
- **What we learned:** versioning should live in one layer (API stage / base path); app routes should be stage-agnostic.
- **Action taken:** normalised route keys and configured Mangum base-path handling.
- **Operational rule:** smoke test `/health` and one versioned data route immediately after every API deploy.

### 5) Schema drift must be handled defensively in serving SQL

- **Issue:** `/picks/today` failed because `stock_picks.score` was absent in the live DB schema.
- **What we learned:** serving SQL should tolerate non-critical column drift in the MVP.
- **Action taken:** returned `NULL::numeric AS score` instead of hard-selecting a missing column.
- **Operational rule:** add a pre-deploy schema compatibility check for serving queries.

### 6) Fetch / ingest decoupling is the right architecture

- **Issue (historical):** the monolithic fetch+write path increased blast radius and retry cost.
- **What we learned:** separation of concerns is essential — stateless fetchers outside the VPC, stateful ingest inside the VPC.
- **Action taken:** kept fetchers no-VPC (external egress only); ingest and serving are VPC-attached (private RDS path).
- **Operational rule:** preserve this boundary; do not mix external API egress and private DB I/O in the same function unless strictly required.
