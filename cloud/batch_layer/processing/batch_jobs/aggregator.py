"""
Scanner Aggregator — AWS Batch Job

Reads every signal for scan_date from daily_scan_signals, ranks globally across
the full universe, writes the final ranked picks to stock_picks, then clears the
staging table for that day. This is the only batch phase the live Step Functions
pipeline invokes (state: RunScannerAggregator).

The scanner Lambda (dev-batch-scanner) writes staging rows in a single Polars
pass. Ranking and writing final picks live here — the aggregator's job, not the
scanner's.
"""

import os
import sys
import logging
import json
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))

from shared.analytics_core.models import SignalResult
from shared.clients.rds_timescale_client import RDSTimescaleClient
from shared.database.staging import daily_scan_signals_ddl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def ensure_daily_scan_signals_table(rds_client: RDSTimescaleClient) -> None:
    """Ensure scanner staging table exists (canonical DDL in shared/database/sql/)."""
    rds_client.execute_query(daily_scan_signals_ddl())


# ---------------------------------------------------------------------------
# Ranking + writing (the aggregator owns these)
# ---------------------------------------------------------------------------

def rank_signals(
    signals: List[SignalResult],
    top_k: int = 10,
    by_pick_type: bool = False,
    unique_symbol: bool = True,
) -> Union[List[SignalResult], Dict[str, List[SignalResult]]]:
    """
    Rank signals by confidence (dense rank).

    Args:
        signals: List of SignalResult to rank.
        top_k: Maximum dense-rank bucket to include (per strategy_name if by_pick_type=True).
        by_pick_type: If True, group by strategy_name and return dict; else return flat list.
        unique_symbol: If True, only one signal per symbol.
    """
    if not signals:
        return {} if by_pick_type else []

    def rank_dense_confidence(items: List[SignalResult]) -> List[SignalResult]:
        scored: List[tuple[float, SignalResult]] = [
            (float(signal.confidence or 0.0), signal) for signal in items
        ]
        scored.sort(key=lambda x: x[0], reverse=True)

        ranked_items: List[SignalResult] = []
        seen_symbols: set[str] = set()
        current_rank = 0
        previous_confidence: Optional[float] = None
        for confidence, signal in scored:
            if previous_confidence is None or confidence != previous_confidence:
                current_rank += 1
                previous_confidence = confidence
            if current_rank > top_k:
                break
            if unique_symbol and signal.symbol in seen_symbols:
                continue
            metadata = dict(signal.metadata or {})
            metadata["ranking_score"] = confidence
            metadata["dense_rank"] = current_rank
            signal.metadata = metadata
            ranked_items.append(signal)
            seen_symbols.add(signal.symbol)
        return ranked_items

    if by_pick_type:
        grouped_input: Dict[str, List[SignalResult]] = defaultdict(list)
        for signal in signals:
            pick_type = (signal.metadata or {}).get("strategy_name", "unclassified")
            grouped_input[pick_type].append(signal)
        return {
            pick_type: rank_dense_confidence(group_signals)
            for pick_type, group_signals in grouped_input.items()
        }

    return rank_dense_confidence(signals)


def write_picks(
    ranked: Union[List[SignalResult], Dict[str, List[SignalResult]]],
    rds_client: Any,
    scan_date: date,
) -> int:
    """Write ranked top picks to stock_picks. Returns rows written."""
    if not ranked:
        return 0

    conn = None
    if hasattr(rds_client, "connection"):
        conn = rds_client.connection
    elif hasattr(rds_client, "conn"):
        conn = rds_client.conn

    ranked_dict = ranked if isinstance(ranked, dict) else {"unclassified": ranked}

    values = []
    for strategy_name, picks in ranked_dict.items():
        for rank_idx, signal in enumerate(picks, start=1):
            metadata_json = json.dumps(signal.metadata or {})
            values.append((
                signal.date or scan_date.isoformat(),
                signal.symbol,
                strategy_name,
                signal.signal,
                signal.price,
                float(signal.confidence or 0.0),
                metadata_json,
                rank_idx,
            ))

    if not values:
        return 0

    query = """
    INSERT INTO stock_picks
    (scan_date, symbol, strategy_name, signal, price, confidence, metadata, rank)
    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    ON CONFLICT (scan_date, symbol, strategy_name)
    DO UPDATE SET
        signal = EXCLUDED.signal,
        price = EXCLUDED.price,
        confidence = EXCLUDED.confidence,
        metadata = EXCLUDED.metadata,
        rank = EXCLUDED.rank
    """

    if conn:
        with conn.cursor() as cur:
            cur.executemany(query, values)
        conn.commit()
    elif hasattr(rds_client, "execute_query"):
        for v in values:
            rds_client.execute_query(query, v)
    else:
        raise ValueError("Unsupported rds_client")

    return len(values)


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------

def run_aggregator(
    scan_date: date,
    strategy_names: Optional[List[str]],
) -> int:
    """
    Read all signals for scan_date from daily_scan_signals, rank globally,
    write final picks to stock_picks, then clean up the staging table.

    Returns:
        Number of top-pick rows written.
    """
    logger.info("=" * 70)
    logger.info(f"SCANNER AGGREGATOR  date={scan_date}")
    logger.info("=" * 70)

    rds_client = RDSTimescaleClient(secret_arn=os.environ.get('RDS_SECRET_ARN'))
    ensure_daily_scan_signals_table(rds_client)

    # ----- read raw signals -----
    filter_clause = ""
    params: tuple = (scan_date.isoformat(),)
    if strategy_names:
        placeholders  = ", ".join(["%s"] * len(strategy_names))
        filter_clause = f"AND strategy_name IN ({placeholders})"
        params        = (scan_date.isoformat(), *strategy_names)

    rows = rds_client.execute_query(
        f"""
        SELECT symbol, scan_date::text AS date, strategy_name,
               signal, price, confidence, metadata
        FROM   daily_scan_signals
        WHERE  scan_date = %s {filter_clause}
        """,
        params,
    )

    if not rows:
        logger.warning(f"No signals found in daily_scan_signals for {scan_date}. "
                       "Did the scanner run?")
        return 0

    logger.info(f"Read {len(rows)} raw signals from staging table")
    strategy_counts = Counter(r["strategy_name"] for r in rows)
    distinct_symbols = len({r["symbol"] for r in rows})
    logger.info(
        "AGGREGATOR input summary: distinct_symbols=%s strategy_counts=%s",
        distinct_symbols,
        dict(strategy_counts),
    )

    # Reconstruct SignalResult objects so rank_signals() can process them
    signals: List[SignalResult] = [
        SignalResult(
            symbol       = r['symbol'],
            date         = r['date'],
            signal       = r['signal'],
            price        = float(r['price']),
            setup_valid  = True,
            trigger_met  = True,
            confidence   = float(r['confidence']) if r['confidence'] is not None else None,
            metadata     = r['metadata'] if isinstance(r['metadata'], dict)
                           else json.loads(r['metadata'] or '{}'),
        )
        for r in rows
    ]

    # ----- global rank -----
    ranked = rank_signals(signals, by_pick_type=True, top_k=10, unique_symbol=True)

    total = (
        sum(len(picks) for picks in ranked.values())
        if isinstance(ranked, dict) else len(ranked)
    )
    logger.info(f"Ranked {total} top picks across {len(ranked)} strategy group(s)")
    if isinstance(ranked, dict):
        preview = {
            k: [
                {
                    "symbol": s.symbol,
                    "confidence": s.confidence,
                    "strategy_name": (s.metadata or {}).get("strategy_name"),
                }
                for s in v[:3]
            ]
            for k, v in ranked.items()
        }
    else:
        preview = [
            {
                "symbol": s.symbol,
                "confidence": s.confidence,
                "strategy_name": (s.metadata or {}).get("strategy_name"),
            }
            for s in ranked[:10]
        ]
    logger.info("AGGREGATOR ranked preview: %s", preview)

    # ----- write stock_picks -----
    picks_written = write_picks(ranked, rds_client, scan_date)
    logger.info(f"Wrote {picks_written} rows to stock_picks")
    stock_picks_count = rds_client.execute_query(
        """
        SELECT COUNT(*) AS cnt
        FROM stock_picks
        WHERE scan_date = %s
        """,
        (scan_date.isoformat(),),
    )[0]["cnt"]
    logger.info(
        "AGGREGATOR post-write stock_picks rows for %s: %s",
        scan_date,
        stock_picks_count,
    )

    # ----- clean up staging rows for today -----
    try:
        conn = rds_client.connection
        old_autocommit = conn.autocommit
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM daily_scan_signals WHERE scan_date = %s",
                    (scan_date.isoformat(),),
                )
                deleted = cur.rowcount
            conn.commit()
            logger.info(f"Cleaned up {deleted} staging rows from daily_scan_signals")
        finally:
            conn.autocommit = old_autocommit
    except Exception as e:
        logger.warning(f"Staging cleanup failed (non-fatal): {e}")

    rds_client.close()

    logger.info("=" * 70)
    logger.info("AGGREGATOR COMPLETE")
    logger.info("=" * 70)
    return picks_written


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """
    AWS Batch entry point for the scanner aggregator.

    Environment variables
    ─────────────────────
    SCAN_DATE         YYYY-MM-DD   (default: today)
    STRATEGY_NAME     comma-separated list or empty for ALL
    """
    logger.info("=" * 70)
    logger.info("AWS BATCH SCANNER AGGREGATOR STARTUP")
    logger.info("=" * 70)

    aws_region = os.environ.get('AWS_REGION', 'ca-west-1')

    # SCAN_DATE injected by Step Functions as the execution-start date (YYYY-MM-DD)
    scan_date_str = os.environ.get('SCAN_DATE', '').strip()
    scan_date: date = date.today()
    if scan_date_str:
        try:
            scan_date = datetime.strptime(scan_date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"Invalid SCAN_DATE '{scan_date_str}' — using today.")

    # Optional strategy filter (comma-separated or empty → ALL)
    strategy_names_env = os.environ.get('STRATEGY_NAME', '').strip()
    strategy_names: Optional[List[str]] = (
        [s.strip() for s in strategy_names_env.split(',') if s.strip()]
        if strategy_names_env else None
    )

    logger.info(f"AWS_REGION:    {aws_region}")
    logger.info(f"SCAN_DATE:     {scan_date}")
    logger.info(f"STRATEGIES:    {strategy_names or 'ALL'}")

    try:
        result = run_aggregator(scan_date, strategy_names)
        logger.info(f"Aggregator done — {result} top picks written.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"FATAL: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
