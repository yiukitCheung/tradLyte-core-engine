# Serving layer

REST-facing APIs for the TradLyte frontend. The full HTTP contract (routes, params, payloads, auth, CORS) lives in [`API_GUIDE.md`](API_GUIDE.md).

The serving API is a single VPC Lambda (`dev-serving-api`, FastAPI + Mangum) behind an HTTP API Gateway (stage `v1`). `POST /v1/backtest` proxies execution to a separate container Lambda (`dev-serving-backtester`) so heavy analytics deps stay out of the main serving zip.

## Routes (live in dev)

| Route | Description |
|---|---|
| `GET /v1/health` | Liveness probe (no API key) |
| `GET /v1/screener/quotes` | Filtered universe with latest daily OHLCV |
| `GET /v1/picks/today` | Ranked picks from the latest `scan_date` |
| `GET /v1/picks/today/metadata` | Same, with the `metadata` JSON column |
| `GET /v1/picks/detail` | One symbol + scan date, joined with `symbol_metadata` |
| `GET /v1/picks/{scan_date}/returns` | Per-pick return horizons (default 1d / 5d / 21d) |
| `GET /v1/market/quote/{symbol}` | Latest daily bar + metadata |
| `GET /v1/market/news/{symbol}` | Polygon news feed |
| `GET /v1/market/ohlcv/{symbol}` | OHLCV history by interval |
| `GET /v1/market/returns/{symbol}` | Multi-horizon returns from daily closes |
| `POST /v1/backtest` | Single-symbol strategy backtest |

## Code layout

| Path | Role |
|---|---|
| `lambda_functions/serving_api/` | FastAPI + Mangum serving Lambda (`dev-serving-api`) |
| `lambda_functions/serving_api/routers/` | Route handlers (`screener`, `picks`, `market`, `backtest`) |
| `lambda_functions/serving_api/{db,cache,models}.py` | RDS access, in-process LRU cache, response models |
| `lambda_functions/backtester/` | Container Lambda (`backtest_handler.py`, `Dockerfile`, requirements) |
| `infrastructure/serving_api/` | `deploy_lambda.sh` + `deploy_http_api.sh` |
| `infrastructure/backtester/build_push_backtester.sh` | Build & push backtester image to ECR |

## Deploy

Serving API (from repo root):

```bash
./cloud/serving_layer/infrastructure/serving_api/deploy_lambda.sh
./cloud/serving_layer/infrastructure/serving_api/deploy_http_api.sh
```

Backtester container:

```bash
AWS_REGION=ca-west-1 ECR_REPO=dev-serving-backtester \
  ./cloud/serving_layer/infrastructure/backtester/build_push_backtester.sh

# After a code change, point the Lambda at the new image:
aws lambda update-function-code \
  --function-name dev-serving-backtester \
  --image-uri "$(aws sts get-caller-identity --query Account --output text).dkr.ecr.ca-west-1.amazonaws.com/dev-serving-backtester:latest" \
  --region ca-west-1
```

Backtester env vars: `RDS_SECRET_ARN` (use the RDS Proxy endpoint as `host`), `BACKTEST_MAX_LOOKBACK_DAYS` (default `1825`). Environment variables, RDS Proxy setup, and the SG matrix are in [`infrastructure/serving_api/README.md`](infrastructure/serving_api/README.md). Backtest request/response schema and exit types are in [`API_GUIDE.md`](API_GUIDE.md).
