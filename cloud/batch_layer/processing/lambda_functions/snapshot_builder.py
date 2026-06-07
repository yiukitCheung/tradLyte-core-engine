"""
Scanner Snapshot Builder Lambda

Maintains a single consolidated, long-format Parquet snapshot of the entire
1d OHLCV universe so the scanner can pull the whole market into
RAM with one S3 GET and run strategies via ``.over("symbol")``.

Why a dedicated snapshot (vs. scanning bronze directly)
-------------------------------------------------------
Bronze is stored as ``bronze/raw_ohlcv/symbol=X/date=Y.parquet`` — one tiny
object per symbol-day. A full-universe scan over that layout would force tens
of thousands of S3 LIST/GET calls against tiny files, where per-request
latency dominates and Parquet's columnar advantages evaporate (the
"small-file problem"). This Lambda builds one large columnar file ONCE
(write amplification off the hot path) that the scanner reads cheaply many
times.

Schema (long / stacked form — optimal for window functions)
-----------------------------------------------------------
    [date: Date, symbol: Utf8, open, high, low, close, volume: Float64]

One row per (date, symbol). 3d / long-term bars are NOT persisted — they are
derived on the fly by the scanner from this 1d base, so the snapshot stays a
single source of truth.

Parquet is immutable — you cannot append rows in place. The daily "append"
is therefore a read-existing + concat + full rewrite to a fresh dated key
(cheap at ~100 MB compressed for ~10k symbols x 5y).

De-duplication (one row per symbol-day)
---------------------------------------
``raw_ohlcv`` can hold more than one row that collapses to the same
``(symbol, timestamp::date)`` for ``interval='1d'`` — e.g. two intraday
timestamps, a re-ingest, or a correction. Both reads use
``SELECT DISTINCT ON (symbol, timestamp::date) ... ORDER BY symbol,
timestamp::date, timestamp DESC`` so Postgres keeps exactly the latest bar per
symbol-day at the source (bounded, no client-side global ``unique()``). This
guarantees the snapshot has a single row per ``(symbol, date)``.

Modes
-----
- ``bootstrap``   : full rebuild from RDS raw_ohlcv (year-chunked, server-side
                    cursor to bound memory). Use this once to create the file,
                    or to re-clean history after a dedupe-logic change.
- ``incremental`` : (default) read the latest snapshot from S3, pull only the
                    deduped new bars for ``scan_date`` from RDS, drop that
                    date's existing rows, concat, trim to the retention window,
                    rewrite. Falls back to bootstrap if no snapshot exists.

Storage layout
--------------
- ``<prefix>/latest/<file>``         : stable key the scanner always reads.
- ``<prefix>/history/<date>/<file>`` : point-in-time dated copies. Kept under a
                    dedicated ``history/`` subprefix so an S3 lifecycle rule can
                    expire them (e.g. after 14 days) WITHOUT ever touching the
                    permanent ``latest/`` object.

Invoked by Step Functions (after OHLCV ingest) or manually:
  Payload:
    mode:        "bootstrap" | "incremental"   (optional; default incremental)
    scan_date:   "YYYY-MM-DD"                  (optional; incremental target, default today UTC)
    bootstrap_start / bootstrap_end: "YYYY-MM-DD" (optional bootstrap bounds)

Environment:
    RDS_SECRET_ARN            Secrets Manager secret with RDS credentials (required)
    S3_BUCKET_NAME            Datalake bucket            (default: dev-condvest-datalake)
    SNAPSHOT_PREFIX           Key prefix                 (default: scanner-snapshots)
    SNAPSHOT_FILENAME         File name                  (default: market_1d.parquet)
    SNAPSHOT_RETENTION_DAYS   Rolling window to keep     (default: 1825 ~ 5y)
"""

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import boto3
import polars as pl
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUCKET = os.environ.get("S3_BUCKET_NAME", "dev-condvest-datalake")
SNAPSHOT_PREFIX = os.environ.get("SNAPSHOT_PREFIX", "scanner-snapshots")
SNAPSHOT_FILENAME = os.environ.get("SNAPSHOT_FILENAME", "market_1d.parquet")
RETENTION_DAYS = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "1825"))

# The scanner always reads this stable key. Dated copies live under a dedicated
# history/ subprefix so a lifecycle rule can expire them without touching this.
LATEST_KEY = f"{SNAPSHOT_PREFIX}/latest/{SNAPSHOT_FILENAME}"
HISTORY_PREFIX = f"{SNAPSHOT_PREFIX}/history"

# Polars schema for frames built from raw psycopg2 tuples. The SQL casts
# numeric columns to float8 so we never see Decimal here.
SNAPSHOT_SCHEMA: Dict[str, Any] = {
    "symbol": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}
# Canonical column order written to Parquet (date first for scan locality).
SNAPSHOT_COLUMNS: List[str] = ["date", "symbol", "open", "high", "low", "close", "volume"]

# Server-side cursor fetch batch size (rows) — bounds client memory on bootstrap.
FETCH_BATCH = 500_000

# Lambda ephemeral storage (/tmp) scratch paths. Reading/writing the snapshot
# via files (instead of in-memory BytesIO) avoids holding the raw bytes and the
# decompressed frame simultaneously, which matters for a ~10M-row file.
LOCAL_EXISTING = "/tmp/snapshot_existing.parquet"
LOCAL_OUT = "/tmp/snapshot_out.parquet"


# ---------------------------------------------------------------------------
# RDS / S3 helpers
# ---------------------------------------------------------------------------

try:
    from clients.rds_connection import get_rds_connection_string
except ImportError:  # pragma: no cover - local dev
    from shared.clients.rds_connection import get_rds_connection_string  # type: ignore


def _read_rows_to_frame(conn, query: str, params: tuple, server_side: bool) -> pl.DataFrame:
    """
    Run a SELECT and build a Polars frame.

    When ``server_side`` is True a named (server-side) cursor streams rows in
    ``FETCH_BATCH`` chunks so a multi-million-row bootstrap query never
    buffers the full result client-side. Each chunk is converted to Polars and
    the raw tuples are released between batches.
    """
    if server_side:
        # Named cursors must run inside a transaction; psycopg2 handles that
        # implicitly when autocommit is False (the default).
        cur_name = f"snapshot_stream_{int(datetime.utcnow().timestamp() * 1000)}"
        cur = conn.cursor(name=cur_name)
        cur.itersize = FETCH_BATCH
    else:
        cur = conn.cursor()

    try:
        cur.execute(query, params)
        frames: List[pl.DataFrame] = []
        while True:
            rows: List[Tuple] = cur.fetchmany(FETCH_BATCH)
            if not rows:
                break
            # Tuple order matches the SELECT column order below.
            frames.append(
                pl.DataFrame(
                    rows,
                    schema={
                        "symbol": pl.Utf8,
                        "date": pl.Date,
                        "open": pl.Float64,
                        "high": pl.Float64,
                        "low": pl.Float64,
                        "close": pl.Float64,
                        "volume": pl.Float64,
                    },
                    orient="row",
                )
            )
            del rows
    finally:
        cur.close()

    if not frames:
        return pl.DataFrame(schema=SNAPSHOT_SCHEMA)
    return pl.concat(frames, how="vertical") if len(frames) > 1 else frames[0]


def _download_existing(s3) -> Optional[str]:
    """
    Download the current latest snapshot to /tmp and return its local path, or
    None if no snapshot exists yet. Reading from a file lets Polars memory-map /
    stream it rather than decompressing a full in-memory byte buffer.
    """
    try:
        s3.download_file(BUCKET, LATEST_KEY, LOCAL_EXISTING)
        return LOCAL_EXISTING
    except Exception as e:
        if any(tok in str(e) for tok in ("NoSuchKey", "404", "Not Found")):
            return None
        raise


def _normalize(df: pl.DataFrame) -> pl.DataFrame:
    """Enforce canonical column order, sort, and stable dtypes."""
    if df.is_empty():
        return df.select([c for c in SNAPSHOT_COLUMNS if c in df.columns])
    return df.select(SNAPSHOT_COLUMNS).sort(["symbol", "date"])


def _trim_retention(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows older than the rolling retention window (relative to max date)."""
    if df.is_empty():
        return df
    max_date = df["date"].max()
    cutoff = max_date - timedelta(days=RETENTION_DAYS)
    return df.filter(pl.col("date") >= cutoff)


def _write_snapshot(s3, df: pl.DataFrame, scan_date: date) -> Dict[str, str]:
    """
    Write the snapshot to a dated key (point-in-time) and overwrite the stable
    latest key that the scanner reads. Writes once to /tmp then uploads the same
    file to both keys (no second in-memory copy).
    """
    df.write_parquet(LOCAL_OUT, compression="zstd")
    size_mb = os.path.getsize(LOCAL_OUT) / 1e6

    dated_key = f"{HISTORY_PREFIX}/{scan_date.isoformat()}/{SNAPSHOT_FILENAME}"
    for key in (dated_key, LATEST_KEY):
        s3.upload_file(LOCAL_OUT, BUCKET, key)
        logger.info("Wrote snapshot → s3://%s/%s (%.1f MB)", BUCKET, key, size_mb)

    try:
        os.remove(LOCAL_OUT)
    except OSError:
        pass

    return {"dated_key": dated_key, "latest_key": LATEST_KEY}


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

# Columns from raw_ohlcv, numeric cast to float8 so psycopg2 never returns
# Decimal (which Polars would reject against the Float64 schema).
_SELECT_COLS = (
    "symbol, timestamp::date AS date, "
    "open::float8 AS open, high::float8 AS high, low::float8 AS low, "
    "close::float8 AS close, volume::float8 AS volume"
)

# DISTINCT ON keeps exactly one row per (symbol, calendar day): the one with the
# latest intraday timestamp. The leading ORDER BY columns MUST match the
# DISTINCT ON list; ``timestamp DESC`` then selects the most recent bar.
_DISTINCT_ON = "DISTINCT ON (symbol, timestamp::date)"
_DEDUPE_ORDER = "ORDER BY symbol, timestamp::date, timestamp DESC"


def run_bootstrap(
    conn,
    s3,
    scan_date: date,
    start: Optional[date],
    end: Optional[date],
) -> Dict[str, Any]:
    """Full rebuild from RDS, read year-by-year to bound memory."""
    # Resolve bounds. Default end = scan_date; default start = end - retention.
    end = end or scan_date
    start = start or (end - timedelta(days=RETENTION_DAYS))
    logger.info("BOOTSTRAP from RDS  range=[%s, %s]", start, end)

    yearly_frames: List[pl.DataFrame] = []
    window_start = start
    while window_start <= end:
        window_end = min(date(window_start.year, 12, 31), end)
        # Exclusive upper bound on timestamp to capture the whole end day.
        upper_exclusive = window_end + timedelta(days=1)
        query = (
            f"SELECT {_DISTINCT_ON} {_SELECT_COLS} FROM raw_ohlcv "
            "WHERE interval = '1d' AND timestamp >= %s AND timestamp < %s "
            f"{_DEDUPE_ORDER}"
        )
        frame = _read_rows_to_frame(
            conn, query, (window_start, upper_exclusive), server_side=True
        )
        logger.info("  loaded %s rows for %s..%s", frame.height, window_start, window_end)
        if not frame.is_empty():
            yearly_frames.append(frame)
        window_start = date(window_start.year + 1, 1, 1)

    if not yearly_frames:
        logger.warning("BOOTSTRAP produced zero rows — nothing written.")
        return {"status": "empty", "mode": "bootstrap", "rows_total": 0}

    df = pl.concat(yearly_frames, how="vertical")
    df = _normalize(_trim_retention(df))
    keys = _write_snapshot(s3, df, scan_date)

    return {
        "status": "success",
        "mode": "bootstrap",
        "rows_total": df.height,
        "rows_added": df.height,
        "symbols": df["symbol"].n_unique(),
        "date_min": str(df["date"].min()),
        "date_max": str(df["date"].max()),
        **keys,
    }


def run_incremental(conn, s3, scan_date: date) -> Dict[str, Any]:
    """
    Replace scan_date's rows in the latest snapshot with fresh bars, then rewrite.

    Memory note: an incremental only touches ONE date, so instead of a global
    ``unique()`` over ~10M rows (which spikes RAM well past the Lambda limit) we
    lazily scan the existing file, drop just that date's rows, and append the
    new bars. New bars therefore win for (symbol, scan_date) — corrections too.
    """
    path = _download_existing(s3)
    if path is None:
        logger.info("No existing snapshot found — falling back to BOOTSTRAP.")
        return run_bootstrap(conn, s3, scan_date, start=None, end=scan_date)

    query = (
        f"SELECT {_DISTINCT_ON} {_SELECT_COLS} FROM raw_ohlcv "
        "WHERE interval = '1d' AND timestamp::date = %s "
        f"{_DEDUPE_ORDER}"
    )
    new_bars = _read_rows_to_frame(conn, query, (scan_date,), server_side=False)
    logger.info("INCREMENTAL scan_date=%s — pulled %s new 1d bars from RDS", scan_date, new_bars.height)

    if new_bars.is_empty():
        existing_rows = (
            pl.scan_parquet(path).select(pl.len()).collect().item()
        )
        logger.warning(
            "No new bars in raw_ohlcv for %s — snapshot left unchanged. "
            "(Wrong date vs RDS, or OHLCV ingest lag?)",
            scan_date,
        )
        return {
            "status": "noop",
            "mode": "incremental",
            "rows_total": existing_rows,
            "rows_added": 0,
            "latest_key": LATEST_KEY,
        }

    # Stream the existing file, dropping any rows for scan_date, then append the
    # fresh bars. Streaming keeps the peak working set well under the full frame.
    kept = (
        pl.scan_parquet(path)
        .filter(pl.col("date") != scan_date)
        .collect(streaming=True)
    )
    combined = pl.concat([_normalize(kept), _normalize(new_bars)], how="vertical")
    del kept
    combined = _normalize(_trim_retention(combined))

    keys = _write_snapshot(s3, combined, scan_date)
    result = {
        "status": "success",
        "mode": "incremental",
        "rows_total": combined.height,
        "rows_added": new_bars.height,
        "symbols": combined["symbol"].n_unique(),
        "date_min": str(combined["date"].min()),
        "date_max": str(combined["date"].max()),
        **keys,
    }
    try:
        os.remove(path)
    except OSError:
        pass
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _resolve_scan_date(event: Dict[str, Any]) -> date:
    raw = (event.get("scan_date") or os.environ.get("SCAN_DATE", "")).strip()
    if raw:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            logger.warning("Invalid scan_date '%s' — using today (UTC).", raw)
    return datetime.utcnow().date()


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError:
        logger.warning("Invalid date '%s' — ignoring.", value)
        return None


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    event = event or {}
    logger.info("Event: %s", json.dumps(event, default=str))

    mode = (event.get("mode") or "incremental").strip().lower()
    scan_date = _resolve_scan_date(event)

    logger.info(
        "Snapshot builder  mode=%s  scan_date=%s  bucket=%s  prefix=%s",
        mode, scan_date, BUCKET, SNAPSHOT_PREFIX,
    )

    s3 = boto3.client("s3")
    conn = psycopg2.connect(get_rds_connection_string())
    try:
        if mode == "bootstrap":
            result = run_bootstrap(
                conn, s3, scan_date,
                start=_parse_date(event.get("bootstrap_start")),
                end=_parse_date(event.get("bootstrap_end")),
            )
        else:
            result = run_incremental(conn, s3, scan_date)
    finally:
        conn.close()

    result["scan_date"] = scan_date.isoformat()
    result["bucket"] = BUCKET
    result["statusCode"] = 200
    logger.info("Done: %s", json.dumps(result, default=str))
    return result
