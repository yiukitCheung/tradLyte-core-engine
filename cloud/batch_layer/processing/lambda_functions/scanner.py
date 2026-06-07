"""
Scanner Lambda (dev-batch-scanner)

Reads the consolidated long-format snapshot
(scanner-snapshots/latest/market_1d.parquet), runs every registered strategy
across the entire universe in one Polars pass, and writes raw BUY signals to
``daily_scan_signals`` — the staging table the aggregator reads.

Scoring logic lives in ``analytics_core.scanner`` (bundled with this deployment).

Invoked by Step Functions (after BuildScannerSnapshot):
  Payload:
    scan_date: "YYYY-MM-DD"   (defaults to snapshot max date)
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

# analytics_core is bundled at deploy time. Use the package import so this file
# can also be named scanner.py without shadowing the library module.
try:
    from analytics_core import scanner as ac_scanner
    from clients.rds_connection import get_rds_connection_string
    from database.staging import ensure_daily_scan_signals
except ImportError:  # pragma: no cover - local dev
    from shared.analytics_core import scanner as ac_scanner  # type: ignore
    from shared.clients.rds_connection import get_rds_connection_string  # type: ignore
    from shared.database.staging import ensure_daily_scan_signals  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ.get("S3_BUCKET_NAME", "dev-condvest-datalake")
SNAPSHOT_PREFIX = os.environ.get("SNAPSHOT_PREFIX", "scanner-snapshots")
SNAPSHOT_FILENAME = os.environ.get("SNAPSHOT_FILENAME", "market_1d.parquet")
LATEST_KEY = f"{SNAPSHOT_PREFIX}/latest/{SNAPSHOT_FILENAME}"
SCAN_WINDOW_DAYS = int(os.environ.get("SCAN_WINDOW_DAYS", "1095"))  # 3 years

LOCAL_SNAPSHOT = "/tmp/scanner_snapshot.parquet"
WORKER_IDX = 0


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

    s3 = boto3.client("s3")
    s3.download_file(BUCKET, LATEST_KEY, LOCAL_SNAPSHOT)
    snapshot = pl.read_parquet(LOCAL_SNAPSHOT)
    snapshot_max = snapshot["date"].max()
    scan_date = _resolve_scan_date(event, snapshot_max)
    logger.info(
        "Snapshot rows=%s symbols=%s range=%s..%s  scan_date=%s",
        snapshot.height, snapshot["symbol"].n_unique(), snapshot["date"].min(), snapshot_max, scan_date,
    )

    window_start = scan_date - timedelta(days=SCAN_WINDOW_DAYS)
    base = snapshot.filter(
        (pl.col("date") >= window_start) & (pl.col("date") <= scan_date)
    ).sort(["symbol", "date"])
    logger.info("Windowed base rows=%s (>= %s)", base.height, window_start)

    strategies = event.get("strategies") or list(ac_scanner.STRATEGY_REGISTRY.keys())

    all_rows: List[tuple] = []
    per_strategy: Dict[str, int] = {}
    for name in strategies:
        if name not in ac_scanner.STRATEGY_REGISTRY:
            logger.warning("Unknown strategy '%s' — skipping.", name)
            continue
        _, timeframes = ac_scanner.STRATEGY_REGISTRY[name]
        scored = ac_scanner.score_multi_timeframe(base, name, scan_date)
        rows = _build_rows(scored, name, timeframes, scan_date)
        per_strategy[name] = len(rows)
        all_rows.extend(rows)
        logger.info("Strategy %s -> %s BUY signals", name, len(rows))

    conn = psycopg2.connect(get_rds_connection_string())
    try:
        with conn.cursor() as cur:
            ensure_daily_scan_signals(cur)
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
