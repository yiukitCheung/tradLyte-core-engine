# TradLyte Serving API — HTTP guide

FastAPI app packaged as Lambda (`dev-serving-api` by default), exposed through **Amazon API Gateway HTTP API**. Most routes are GET; backtest is POST.

---

## Base URL

After running `infrastructure/serving_api/deploy_http_api.sh`, the script prints:

```text
https://{API_ID}.execute-api.{AWS_REGION}.amazonaws.com/{STAGE_NAME}
```

| Setting | Default (script) |
|--------|---------------------|
| Region | `ca-west-1` |
| Stage | `v1` (`STAGE_NAME`) |
| API name | `dev-serving-http-api` (`API_NAME`) |

**Example**

```text
https://abc123xyz.execute-api.ca-west-1.amazonaws.com/v1
```

Append the route path **without** an extra `/v1` prefix on each resource (the stage is already in the URL). Example:

```text
GET https://abc123xyz.execute-api.ca-west-1.amazonaws.com/v1/picks/today
```

To discover `API_ID` again:

```bash
aws apigatewayv2 get-apis --region ca-west-1 \
  --query "Items[?Name=='dev-serving-http-api'].ApiId | [0]" --output text
```

---

## Authentication

- If the Lambda has **`SERVING_API_KEY`** or **`SERVING_API_KEY_SECRET_ARN`** set, every **protected** route requires header:

  ```http
  x-api-key: <your-key>
  ```

- Secret mode (recommended): set `SERVING_API_KEY_SECRET_ARN` to an AWS Secrets Manager secret containing `SERVING_API_KEY` (or `api_key` / `x_api_key`).
- Env fallback mode: set `SERVING_API_KEY` directly.
- If neither is set, the app does not enforce a key (use only in dev).

- **`GET /health`** is **not** behind the API-key dependency in code; it is intended for load balancers and quick checks. Still register a route in API Gateway if you use `deploy_http_api.sh` (it includes `GET /health`).

API Gateway does **not** validate `x-api-key` natively for HTTP APIs; validation happens inside Lambda.

For browser apps, any key sent from frontend JavaScript is visible to end users. For production, prefer user auth (JWT/Cognito/Lambda authorizer) or route API calls through your own backend/BFF instead of shipping a static shared key in public client code.

---

## CORS

Configured in `deploy_http_api.sh`: allowed methods **GET**, **POST**, **OPTIONS**; allowed headers include **`content-type`** and **`x-api-key`**. Set **`ALLOWED_ORIGIN`** when deploying (e.g. your frontend origin).

---

## Response shapes

### Success (most routes)

```json
{
  "data": {},
  "meta": {}
}
```

Fields vary by endpoint (`cache_hit`, `count`, etc.).

### Errors

Handled routes return JSON such as:

```json
{
  "error": {
    "code": "http_error",
    "message": "..."
  }
}
```

HTTP status reflects the error (`401`, `404`, `500`, …).

---

## Endpoints

Routes must exist in API Gateway — keep **`deploy_http_api.sh`** `ROUTES` array in sync when you add FastAPI paths.

### Health (no API key on app router)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness: `status`, `service`, `timestamp` (UTC) |

### Screener (`/screener` — requires API key if configured)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/screener/quotes` | Filtered universe with latest daily OHLCV as of watermark `as_of` |

**Query parameters** (`/screener/quotes`)

| Param | Type | Default | Notes |
|-------|------|---------|--------|
| `industry` | string | — | Exact match on `symbol_metadata.industry` |
| `type` | string | — | e.g. asset type |
| `min_market_cap` | int | — | `>= 0` |
| `max_market_cap` | int | — | `>= 0` |
| `sort` | string | `marketcap:desc` | `field:asc\|desc`; fields: `marketcap`, `symbol`, `close`, `volume` |
| `limit` | int | `50` | `1–500` |
| `offset` | int | `0` | Pagination |

### Picks (`/picks` — requires API key if configured)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/picks/today` | Latest `scan_date` rows from `stock_picks` (ranked) |
| GET | `/picks/today/metadata` | Same date scope; columns include `metadata` JSON |
| GET | `/picks/detail` | One symbol + scan date, joined with `symbol_metadata` |
| GET | `/picks/{scan_date}/returns` | Per-pick horizons vs `raw_ohlcv` after `scan_date` |

### Backtest (`/backtest` — requires API key if configured)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/backtest` | Run a single-symbol strategy backtest over date range and return performance metrics |

`dev-serving-api` handles request auth/validation and proxies execution to a dedicated backtester Lambda (`BACKTEST_FUNCTION_NAME`, default `dev-serving-backtester`). This keeps serving ZIP packages small while heavy analytics deps live in the containerized backtester function.

**Request body (`/backtest`)**

```json
{
  "strategy_name": "Momentum_Swing",
  "symbol": "AAPL",
  "timeframe": "1d",
  "start_date": "2022-01-01",
  "end_date": "2024-12-31",
  "initial_capital": 10000,
  "components": {
    "setup": { "type": "INDICATOR_THRESHOLD", "timeframe": "1d", "indicator": "RSI", "operator": ">", "value": 50 },
    "trigger": { "type": "CANDLE_PATTERN", "timeframe": "1d", "pattern": "BULLISH_ENGULFING" },
    "exit": { "type": "CONDITIONAL_OR_FIXED", "timeframe": "1d", "conditions": [{ "type": "STOP_LOSS_PCT", "value": 0.05 }, { "type": "TAKE_PROFIT_PCT", "value": 0.12 }] }
  }
}
```

**Response fields (`data`) include:** `total_return_pct`, `sharpe_ratio`, `max_drawdown_pct`, `equity_curve`, `total_trades`, `win_rate`, and `trades`.

**Known-good smoke payload (validated):**

```json
{
  "strategy_name": "smoke_test",
  "symbol": "AAPL",
  "timeframe": "1d",
  "start_date": "2025-01-01",
  "end_date": "2025-01-31",
  "initial_capital": 10000,
  "components": {
    "setup": { "type": "NONE", "timeframe": "1d" },
    "trigger": { "type": "CANDLE_PATTERN", "timeframe": "1d", "pattern": "GREEN_CANDLE" },
    "exit": { "type": "TAKE_PROFIT_PCT", "timeframe": "1d", "value": 0.1 }
  }
}
```

**Example success shape:**

```json
{
  "data": {
    "total_return_pct": -0.0367,
    "sharpe_ratio": -9.1651,
    "max_drawdown_pct": 0.0367,
    "total_trades": 1,
    "equity_curve": [10000, 10000, 9632.98],
    "trades": []
  },
  "meta": {
    "symbol": "AAPL",
    "strategy_name": "smoke_test",
    "timeframe": "1d",
    "start_date": "2025-01-01",
    "end_date": "2025-01-31",
    "source": "dev-serving-backtester"
  }
}
```

**`/picks/today` and `/picks/today/metadata`**

| Param | Type | Default | Notes |
|-------|------|---------|--------|
| `limit` | int | `25` | `1–200` |
| `industry` | string | — | Exact match on `symbol_metadata.industry` (same as screener) |
| `min_market_cap` | int | — | `>= 0`; filter `symbol_metadata.marketcap` |
| `max_market_cap` | int | — | `>= 0`; filter `symbol_metadata.marketcap` |

Picks are joined to `symbol_metadata` on `symbol`. Omitting filters returns the full ranked list for the latest `scan_date` (subject to `limit`). Rows without metadata still appear when **no** industry/cap filters are applied; with filters applied, symbols missing metadata usually drop out (unknown industry/cap).

**`/picks/detail`**

| Param | Type | Required | Notes |
|-------|------|----------|--------|
| `symbol` | string | yes | Ticker |
| `scan_date` | date | yes | `YYYY-MM-DD` |
| `strategy_name` | string | no | Narrow to one strategy |

**`/picks/{scan_date}/returns`**

| Param | Type | Default | Notes |
|-------|------|---------|--------|
| `horizons` | string | `1,5,21` | Comma-separated trading days `1–252` |
| `industry` | string | — | Exact match on `symbol_metadata.industry` |
| `min_market_cap` | int | — | `>= 0`; filter `symbol_metadata.marketcap` |
| `max_market_cap` | int | — | `>= 0`; filter `symbol_metadata.marketcap` |

`{scan_date}` is a path segment, e.g. `2026-04-28`.

### Market (`/market` — requires API key if configured)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/market/quote/{symbol}` | Latest daily bar + metadata |
| GET | `/market/news/{symbol}` | Latest Polygon news for a symbol (live API call) |
| GET | `/market/ohlcv/{symbol}` | OHLCV history |
| GET | `/market/returns/{symbol}` | Simple multi-horizon returns from daily closes |

**`/market/news/{symbol}`**

| Param | Type | Default | Notes |
|-------|------|---------|--------|
| `limit` | int | `10` | `1–50` |
| `order` | string | `desc` | `asc` or `desc` by `published_utc` |
| `published_utc_gte` | date | — | Lower bound filter |
| `published_utc_lte` | date | — | Upper bound filter |

Uses Polygon endpoint `v2/reference/news` with ticker filter and reads API key from AWS Secrets Manager via `POLYGON_API_KEY_SECRET_ARN`. The response includes `data` (articles) and `meta` (count/query params/`next_url` with `apiKey` stripped).

**`/market/ohlcv/{symbol}`**

| Param | Type | Default | Notes |
|-------|------|---------|--------|
| `interval` | string | `1d` | `1d`, `1h`, `15m`, `5m`, `1m` |
| `start_date` | date | — | Filter lower bound |
| `end_date` | date | — | Filter upper bound |
| `limit` | int | `200` | `1–2000` |
| `sort` | string | `desc` | `asc` or `desc` by timestamp |

**`/market/returns/{symbol}`**

| Param | Type | Default |
|-------|------|---------|
| `horizons` | string | `1,5,21` |

---

## Examples (`curl`)

Replace `BASE` and `KEY`.

```bash
BASE="https://abc123xyz.execute-api.ca-west-1.amazonaws.com/v1"
KEY="your-serving-api-key"

curl -sS -H "x-api-key: $KEY" "$BASE/picks/today?limit=10"

curl -sS -H "x-api-key: $KEY" \
  "$BASE/picks/detail?symbol=AAPL&scan_date=2026-04-28"

curl -sS -H "x-api-key: $KEY" \
  "$BASE/market/ohlcv/MSFT?interval=1d&limit=50"

curl -sS -H "x-api-key: $KEY" \
  "$BASE/market/news/AAPL?limit=5&order=desc"

curl -sS -X POST -H "x-api-key: $KEY" -H "Content-Type: application/json" \
  "$BASE/backtest" \
  -d '{"strategy_name":"smoke_test","symbol":"AAPL","timeframe":"1d","start_date":"2025-01-01","end_date":"2025-01-31","initial_capital":10000,"components":{"setup":{"type":"NONE","timeframe":"1d"},"trigger":{"type":"CANDLE_PATTERN","timeframe":"1d","pattern":"GREEN_CANDLE"},"exit":{"type":"TAKE_PROFIT_PCT","timeframe":"1d","value":0.1}}}'

curl -sS "$BASE/health"
```

---

## Deploy checklist when adding routes

1. Implement the route in FastAPI (`cloud/serving_layer/lambda_functions/serving_api/`).
2. Package and update the **Lambda** function code.
3. Add the route key (for example **`GET /path`** or **`POST /path`**) to the `ROUTES` array in `infrastructure/serving_api/deploy_http_api.sh`.
4. Run **`deploy_http_api.sh`** so API Gateway exposes the new path (otherwise you may see `403` / missing route).

---

## Related files

| File | Purpose |
|------|---------|
| `lambda_functions/serving_api/app.py` | FastAPI app, CORS, API-key dependency |
| `lambda_functions/serving_api/routers/*.py` | Route handlers |
| `infrastructure/serving_api/deploy_http_api.sh` | HTTP API + routes + Lambda permission |
