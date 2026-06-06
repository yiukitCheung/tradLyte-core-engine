# Serving API Infrastructure

Scripts to deploy the serving API stack:

- `deploy_lambda.sh` — packages and deploys `dev-serving-api` (FastAPI + Mangum) into the private VPC.
- `deploy_http_api.sh` — creates/updates HTTP API Gateway routes and the `v1` stage (throttling + CORS).

## 1) Deploy Lambda

```bash
AWS_REGION=ca-west-1 \
FUNCTION_NAME=dev-serving-api \
SOURCE_VPC_LAMBDA=dev-batch-daily-ohlcv-ingest-handler \
RDS_SECRET_ARN=<secret-arn> \
SERVING_API_KEY=<key> \
./cloud/serving_layer/infrastructure/serving_api/deploy_lambda.sh
```

## 2) RDS Proxy (AWS Console)

Create `dev-rds-proxy-v2` (PostgreSQL) in the same VPC/subnets as `dev-serving-api`, authenticate via Secrets Manager using your `RDS_SECRET_ARN`, register the DB in the `default` target group, and point the secret `host` at the proxy endpoint once status is **Available**.

Security-group matrix:

- Lambda SG → Proxy SG: outbound TCP 5432
- Proxy SG → DB SG: TCP 5432
- DB SG: inbound TCP 5432 from Proxy SG

## 3) Deploy HTTP API

```bash
AWS_REGION=ca-west-1 \
API_NAME=dev-serving-http-api \
FUNCTION_NAME=dev-serving-api \
STAGE_NAME=v1 \
ALLOWED_ORIGIN=https://app.tradlyte.com \
./cloud/serving_layer/infrastructure/serving_api/deploy_http_api.sh
```

## Environment variables (`dev-serving-api`)

| Var | Purpose |
|---|---|
| `RDS_SECRET_ARN` | Secret with `host`, `port`, `username`, `password`, `database`/`dbname` |
| `SERVING_API_KEY` | Accepted `x-api-key` value (optional but recommended) |
| `ALLOWED_ORIGIN` | Frontend origin for CORS |
| `SCREENER_CACHE_TTL_S` / `RETURNS_CACHE_TTL_S` / `MARKET_CACHE_TTL_S` | Cache TTLs (defaults `60` / `300` / `60`) |
