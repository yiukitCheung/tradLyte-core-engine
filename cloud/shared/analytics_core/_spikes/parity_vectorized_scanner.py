"""
Parity harness: vectorized full-universe scanner vs. per-symbol DailyScanner.

Proves the vectorized path (vectorized_scanner.score_multi_timeframe) selects
the SAME BUY symbols with the SAME confidence as the production per-symbol
scanner (DailyScanner._score_signal), on a real sample drawn from the S3
snapshot — before we build/deploy the runner.

Both paths are fed identical input (same symbols, same 3-year window ending at
scan_date) so any divergence is purely algorithmic, not data.

Run (needs AWS creds to fetch the snapshot once):
    cd cloud/shared
    python -m analytics_core._spikes.parity_vectorized_scanner --symbols 300
"""

from __future__ import annotations

import argparse
import contextlib
import io
import os
import subprocess
from datetime import date, timedelta

import polars as pl

from analytics_core.scanner import DailyScanner
from analytics_core.inputs import build_multi_timeframe_from_batch_1d
from analytics_core.strategies.library import GoldenCrossStrategy, VegasChannelStrategy
from analytics_core.vectorized_scanner import score_multi_timeframe, STRATEGY_REGISTRY

BUCKET = "dev-condvest-datalake"
LATEST_KEY = "scanner-snapshots/latest/market_1d.parquet"
LOCAL = "/tmp/parity_snapshot.parquet"

# Map vectorized registry names -> per-symbol strategy factory + timeframes.
TRUTH_STRATEGIES = {
    "golden_cross": (GoldenCrossStrategy, ["1d", "3d", "5d"]),
    "vegas_channel_short_term": (VegasChannelStrategy, ["1d", "3d", "5d"]),
}


def fetch_snapshot() -> pl.DataFrame:
    if not os.path.exists(LOCAL):
        print(f"Downloading s3://{BUCKET}/{LATEST_KEY} -> {LOCAL} ...")
        subprocess.run(
            ["aws", "s3", "cp", f"s3://{BUCKET}/{LATEST_KEY}", LOCAL, "--region", "ca-west-1"],
            check=True,
        )
    df = pl.read_parquet(LOCAL)
    print(f"Snapshot: {df.height:,} rows, {df['symbol'].n_unique()} symbols, "
          f"{df['date'].min()}..{df['date'].max()}")
    return df


def truth_buys(sample: pl.DataFrame, strategy_name: str, scan_date: date) -> dict:
    """Per-symbol scanner result: {symbol: confidence} of BUY signals."""
    factory, timeframes = TRUTH_STRATEGIES[strategy_name]
    scanner = DailyScanner(rds_connection_string="unused-for-scoring")
    out: dict = {}
    symbols = sample["symbol"].unique().to_list()
    for sym in symbols:
        sym_1d = sample.filter(pl.col("symbol") == sym)
        by_tf = build_multi_timeframe_from_batch_1d(sym_1d, timeframes)
        symbol_data_dict = by_tf.get(sym, {})
        if not symbol_data_dict:
            continue
        # _score_signal is chatty; silence it.
        with contextlib.redirect_stdout(io.StringIO()):
            sig = scanner._score_signal(
                symbol=sym,
                strategy=factory(),
                scan_date=scan_date,
                timeframes=timeframes,
                symbol_data_dict=symbol_data_dict,
            )
        if sig is not None and sig.signal == "BUY":
            out[sym] = round(float(sig.confidence or 0.0), 6)
    return out


def vec_buys(sample: pl.DataFrame, strategy_name: str, scan_date: date) -> dict:
    """Vectorized result: {symbol: confidence}."""
    scored = score_multi_timeframe(sample, strategy_name, scan_date)
    return {
        r["symbol"]: round(float(r["confidence"]), 6)
        for r in scored.iter_rows(named=True)
    }


def compare(name: str, truth: dict, vec: dict) -> bool:
    t_syms, v_syms = set(truth), set(vec)
    only_t = t_syms - v_syms
    only_v = v_syms - t_syms
    common = t_syms & v_syms
    conf_mismatch = {s: (truth[s], vec[s]) for s in common if truth[s] != vec[s]}

    ok = not only_t and not only_v and not conf_mismatch
    print(f"\n=== {name} ===")
    print(f"  per-symbol BUYs: {len(truth)}   vectorized BUYs: {len(vec)}   common: {len(common)}")
    print(f"  identical: {ok}")
    if only_t:
        print(f"  only per-symbol ({len(only_t)}): {sorted(only_t)[:8]}")
    if only_v:
        print(f"  only vectorized ({len(only_v)}): {sorted(only_v)[:8]}")
    if conf_mismatch:
        sample = list(conf_mismatch.items())[:8]
        print(f"  confidence mismatches ({len(conf_mismatch)}): {sample}")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=int, default=300, help="sample size")
    ap.add_argument("--scan-date", type=str, default=None, help="YYYY-MM-DD (default: snapshot max date)")
    ap.add_argument("--years", type=int, default=3, help="history window fed to both paths")
    args = ap.parse_args()

    df = fetch_snapshot()
    scan_date = (
        date.fromisoformat(args.scan_date) if args.scan_date else df["date"].max()
    )
    start = scan_date - timedelta(days=365 * args.years)

    # Deterministic sample of symbols that actually have a bar on scan_date.
    on_date = df.filter(pl.col("date") == scan_date)["symbol"].unique().sort().to_list()
    chosen = on_date[: args.symbols]
    sample = df.filter(
        pl.col("symbol").is_in(chosen) & (pl.col("date") >= start) & (pl.col("date") <= scan_date)
    ).sort(["symbol", "date"])
    print(f"scan_date={scan_date}  window>={start}  sample symbols={len(chosen)}  rows={sample.height:,}")

    all_ok = True
    for strat in STRATEGY_REGISTRY:
        t = truth_buys(sample, strat, scan_date)
        v = vec_buys(sample, strat, scan_date)
        all_ok &= compare(strat, t, v)

    print("\n" + ("ALL STRATEGIES PARITY: PASS" if all_ok else "PARITY: FAIL — see diffs above"))


if __name__ == "__main__":
    main()
