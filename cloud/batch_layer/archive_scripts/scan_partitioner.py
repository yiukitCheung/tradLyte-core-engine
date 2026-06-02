"""
Scanner Partitioner Lambda

Runs once before the scanner Array Job. Its only job:

  1. Query RDS for all active symbols (one cheap query).
  2. Split the list into N equal chunks (default 10).
  3. Write each chunk to S3 as a tiny JSON file:
       s3://{bucket}/scanner-chunks/{scan_date}/chunk_{i}.json
  4. Return metadata so Step Functions can pass it to the next state.

This keeps symbol-list logic out of the Batch workers and means each
Batch container only issues a targeted RDS query for its ~500 symbols
instead of the full universe.

Invoked by Step Functions:
  Resource: arn:aws:states:::lambda:invoke
  Parameters:
    FunctionName: dev-batch-scan-partitioner
    Payload:
      scan_date.$: "<YYYY-MM-DD>"
      array_size: 10          # must match ArrayProperties.Size in state machine
"""

import json
import logging
import os
import math
from datetime import date, datetime
from typing import Any, Dict, List

import boto3

# Shared RDS client is bundled in the Lambda deployment package
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
from shared.clients.rds_timescale_client import RDSTimescaleClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 bucket used to stage chunk files.
# Reuses the same datalake bucket; prefix keeps things isolated.
CHUNKS_BUCKET  = os.environ.get('S3_BUCKET_NAME', 'dev-condvest-datalake')
CHUNKS_PREFIX  = 'scanner-chunks'
DEFAULT_ARRAY_SIZE = 10
REQUIRE_OHLCV_FOR_SCAN_DATE = os.environ.get("REQUIRE_OHLCV_FOR_SCAN_DATE", "true").lower() == "true"

def _get_scannable_symbols(rds_client: RDSTimescaleClient, scan_date: date) -> List[str]:
    """
    Return symbols that are:
      1) active in symbol_metadata, and
      2) have 1d OHLCV loaded for the given scan_date.

    This prevents scanner workers from receiving symbols that cannot be priced
    for the current day due to async ingest lag or stale metadata rows.
    """
    rows = rds_client.execute_query(
        """
        SELECT DISTINCT m.symbol
        FROM symbol_metadata m
        JOIN raw_ohlcv o
          ON o.symbol = m.symbol
         AND o.interval = '1d'
         AND o.timestamp::date = %s
        WHERE LOWER(TRIM(COALESCE(m.active, 'true'))) = 'true'
        ORDER BY m.symbol
        """,
        (scan_date.isoformat(),),
    )
    return [row["symbol"] for row in rows]


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Lambda entry point.

    Expected event keys (all optional — fall back to env / defaults):
      scan_date   YYYY-MM-DD   (default: today in ET)
      array_size  int          (default: DEFAULT_ARRAY_SIZE)
    """
    logger.info(f"Event: {json.dumps(event)}")

    # ── resolve scan_date ─────────────────────────────────────────────────────
    scan_date_str = event.get('scan_date') or os.environ.get('SCAN_DATE', '').strip()
    if scan_date_str:
        try:
            scan_date = datetime.strptime(scan_date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"Invalid scan_date '{scan_date_str}' — using today.")
            scan_date = date.today()
    else:
        scan_date = date.today()

    array_size = int(event.get('array_size', os.environ.get('ARRAY_SIZE', DEFAULT_ARRAY_SIZE)))

    logger.info(f"scan_date={scan_date}  array_size={array_size}  bucket={CHUNKS_BUCKET}")

    # ── fetch symbols for scanner chunks (single RDS query) ───────────────────
    rds_client = RDSTimescaleClient.from_lambda_environment()
    try:
        if REQUIRE_OHLCV_FOR_SCAN_DATE:
            symbols: List[str] = _get_scannable_symbols(rds_client, scan_date)
            logger.info(
                "Fetched %s active symbols with OHLCV on %s",
                len(symbols),
                scan_date.isoformat(),
            )
        else:
            symbols = rds_client.get_active_symbols()
            logger.info("Fetched %s active symbols (metadata-only mode)", len(symbols))
    finally:
        rds_client.close()

    if not symbols:
        logger.warning("No active symbols found — nothing to partition.")
        return {
            'statusCode': 200,
            'scan_date':   scan_date.isoformat(),
            'array_size':  array_size,
            'total_symbols': 0,
            'chunks_written': 0,
            'bucket':      CHUNKS_BUCKET,
            'prefix':      f"{CHUNKS_PREFIX}/{scan_date.isoformat()}",
        }

    # ── split into equal chunks ───────────────────────────────────────────────
    chunk_size = math.ceil(len(symbols) / array_size)
    chunks: List[List[str]] = [
        symbols[i : i + chunk_size]
        for i in range(0, len(symbols), chunk_size)
    ]
    # Pad to exactly array_size chunks so indices are predictable
    while len(chunks) < array_size:
        chunks.append([])

    # ── write chunks to S3 ───────────────────────────────────────────────────
    s3 = boto3.client('s3')
    prefix = f"{CHUNKS_PREFIX}/{scan_date.isoformat()}"

    for idx, chunk in enumerate(chunks):
        key = f"{prefix}/chunk_{idx}.json"
        body = json.dumps({
            'scan_date':   scan_date.isoformat(),
            'chunk_index': idx,
            'array_size':  array_size,
            'symbols':     chunk,
        })
        s3.put_object(
            Bucket      = CHUNKS_BUCKET,
            Key         = key,
            Body        = body,
            ContentType = 'application/json',
        )
        logger.info(f"Wrote chunk {idx}: {len(chunk)} symbols → s3://{CHUNKS_BUCKET}/{key}")

    logger.info(
        f"Partitioning complete: {len(symbols)} symbols → "
        f"{array_size} chunks of ~{chunk_size} in s3://{CHUNKS_BUCKET}/{prefix}/"
    )

    return {
        'statusCode':    200,
        'scan_date':     scan_date.isoformat(),
        'array_size':    array_size,
        'total_symbols': len(symbols),
        'chunks_written': array_size,
        'bucket':        CHUNKS_BUCKET,
        'prefix':        prefix,
    }
