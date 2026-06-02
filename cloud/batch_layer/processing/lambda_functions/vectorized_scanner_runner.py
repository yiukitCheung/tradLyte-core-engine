"""
Vectorized Scanner Runner Lambda

Single-pass replacement for the per-symbol AWS Batch scanner *worker* phase
(the 10-child array job). Reads the consolidated long-format snapshot
(scanner-snapshots/latest/market_1d.parquet), runs every registered strategy
across the ENTIRE universe at once using Polars window functions, and writes
raw BUY signals to ``daily_scan_signals`` — the exact same staging table the
existing Phase-2 aggregator reads. The aggregator (global ranking ->
stock_picks) is unchanged.

Why this exists
---------------
The old worker phase fanned out into N containers, each querying RDS for ~500
symbols and looping symbol-by-symbol through strategies. This Lambda collapses
that into one vectorized scan over an in-memory frame. Offline parity against
``DailyScanner._score_signal`` was validated to produce identical BUY sets and
confidences (golden_cross + vegas_channel_short_term).

Parity-critical details
------------------------
* Window: scores against a trailing 3-year window ending at scan_date — the
  same window ``DailyScanner.run`` uses (scan_date - 3*365 days). Indicator
  warm-up therefore matches the per-symbol path exactly.
* Resampling/scoring live in ``vectorized_scanner`` (bundled alongside this
  handler), which mirrors ``resample_ohlcv`` and ``_score_signal``.

Invoked by Step Functions (after BuildScannerSnapshot):
  Payload:
    scan_date: "YYYY-MM-DD"   (required in pipeline; defaults to snapshot max date)
    strategies: ["golden_cross", ...]  (optional; default = all registered)

Environment:
    RDS_SECRET_ARN     Secrets Manager secret with RDS credentials (required)
    S3_BUCKET_NAME     Datalake bucket            (default: dev-condvest-datalake)
    SNAPSHOT_PREFIX    Snapshot key prefix        (default: scanner-snapshots)
    SNAPSHOT_FILENAME  Snapshot file name         (default: market_1d.parquet)
    SCAN_WINDOW_DAYS   Trailing window fed to scorer (default: 1095 = 3y)
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

import boto3
import polars as pl
import psycopg2

# vectorized_scanner.py is bundled at the package root by the deploy script.
# Fall back to the shared package path for local execution / tests.
try:
    import vectorized_scanner as vs
except ImportError:  # pragma: no cover - local dev
    from shared.analytics_core import vectorized_scanner as vs  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ.get("S3_BUCKET_NAME", "dev-condvest-datalake")
SNAPSHOT_PREFIX = os.environ.get("SNAPSHOT_PREFIX", "scanner-snapshots")
SNAPSHOT_FILENAME = os.environ.get("SNAPSHOT_FILENAME", "market_1d.parquet")
LATEST_KEY = f"{SNAPSHOT_PREFIX}/latest/{SNAPSHOT_FILENAME}"
SCAN_WINDOW_DAYS = int(os.environ.get("SCAN_WINDOW_DAYS", "1095"))  # 3 years

LOCAL_SNAPSHOT = "/tmp/scanner_snapshot.parquet"

# DDL kept byte-identical to batch_jobs/scan.py so the table contract matches.
_DDL = """
CREATE TABLE IF NOT EXISTS daily_scan_signals (
    scan_date     DATE         NOT NULL,
    worker_idx    SMALLINT     NOT NULL,
    symbol        VARCHAR(50)  NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    signal        VARCHAR(10)  NOT NULL CHECK (signal IN ('BUY', 'SELL', 'HOLD')),
    price         DECIMAL(12,4) NOT NULL,
    confidence    DECIMAL(5,4),
    metadata      JSONB        DEFAULT '{}'::jsonb,
    created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scan_date, symbol, strategy_name)
);
"""
_DDL_IDX = "CREATE INDEX IF NOT EXISTS idx_daily_scan_signals_date ON daily_scan_signals(scan_date);"

# Single logical worker (no array fan-out anymore).
WORKER_IDX = 0


def get_rds_connection_string() -> str:
    secret_arn = os.environ.get("RDS_SECRET_ARN")
    if not secret_arn:
        raise ValueError("RDS_SECRET_ARN environment variable not set")
    client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION", "ca-west-1"))
    secret = json.loads(client.get_secret_value(SecretId=secret_arn)["SecretString"])
    host = secret["host"]
    port = secret.get("port", 5432)
    db = secret.get("database", secret.get("dbname", "postgres"))
    user = secret["username"]
    pwd = secret["password"]
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode=require"


def _resolve_scan_date(event: Dict[str, Any], snapshot_max):
    raw = (event.get("scan_date") or os.environ.get("SCAN_DATE", "")).strip()
    if raw:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            logger.warning("Invalid scan_date '%s' — using snapshot max date.", raw)
    return snapshot_max


def _build_rows(scored: pl.DataFrame, strategy_name: str, timeframes: List[str], scan_date) -> List[tuple]:
    """Map a scored frame to daily_scan_signals insert tuples."""
    rows: List[tuple] = []
    for r in scored.iter_rows(named=True):
        metadata = {"strategy_name": strategy_name, "timeframes": timeframes}
        rows.append(
            (
                scan_date.isoformat(),
                WORKER_IDX,
                r["symbol"],
                strategy_name,
                r["signal"],
                float(r["price"]),
                float(r["confidence"]) if r["confidence"] is not None else None,
                json.dumps(metadata),
            )
        )
    return rows


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    event = event or {}
    logger.info("Event: %s", json.dumps(event, default=str))

    # ---- load snapshot ----
    s3 = boto3.client("s3")
    s3.download_file(BUCKET, LATEST_KEY, LOCAL_SNAPSHOT)
    snapshot = pl.read_parquet(LOCAL_SNAPSHOT)
    snapshot_max = snapshot["date"].max()
    scan_date = _resolve_scan_date(event, snapshot_max)
    logger.info(
        "Snapshot rows=%s symbols=%s range=%s..%s  scan_date=%s",
        snapshot.height, snapshot["symbol"].n_unique(), snapshot["date"].min(), snapshot_max, scan_date,
    )

    # ---- trailing window (parity with DailyScanner.run) ----
    window_start = scan_date - timedelta(days=SCAN_WINDOW_DAYS)
    base = snapshot.filter(
        (pl.col("date") >= window_start) & (pl.col("date") <= scan_date)
    ).sort(["symbol", "date"])
    logger.info("Windowed base rows=%s (>= %s)", base.height, window_start)

    strategies = event.get("strategies") or list(vs.STRATEGY_REGISTRY.keys())

    # ---- score every strategy over the full universe ----
    all_rows: List[tuple] = []
    per_strategy: Dict[str, int] = {}
    for name in strategies:
        if name not in vs.STRATEGY_REGISTRY:
            logger.warning("Unknown strategy '%s' — skipping.", name)
            continue
        _, timeframes = vs.STRATEGY_REGISTRY[name]
        scored = vs.score_multi_timeframe(base, name, scan_date)
        rows = _build_rows(scored, name, timeframes, scan_date)
        per_strategy[name] = len(rows)
        all_rows.extend(rows)
        logger.info("Strategy %s -> %s BUY signals", name, len(rows))

    # ---- write to daily_scan_signals ----
    conn = psycopg2.connect(get_rds_connection_string())
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
            cur.execute(_DDL_IDX)
        conn.commit()

        if all_rows:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO daily_scan_signals
                        (scan_date, worker_idx, symbol, strategy_name,
                         signal, price, confidence, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (scan_date, symbol, strategy_name)
                    DO UPDATE SET
                        signal     = EXCLUDED.signal,
                        price      = EXCLUDED.price,
                        confidence = EXCLUDED.confidence,
                        metadata   = EXCLUDED.metadata,
                        worker_idx = EXCLUDED.worker_idx
                    """,
                    all_rows,
                )
            conn.commit()
    finally:
        conn.close()

    result = {
        "statusCode": 200,
        "status": "success",
        "scan_date": scan_date.isoformat(),
        "signals_written": len(all_rows),
        "per_strategy": per_strategy,
        "snapshot_max_date": str(snapshot_max),
        "stale_snapshot": str(snapshot_max) < scan_date.isoformat(),
    }
    logger.info("Done: %s", json.dumps(result, default=str))
    return result
