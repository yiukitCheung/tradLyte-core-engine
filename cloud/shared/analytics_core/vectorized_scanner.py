"""
Vectorized Full-Universe Scanner

Runs a strategy across the ENTIRE symbol universe in a single Polars pass,
instead of looping one symbol at a time. The input is the consolidated
long-format snapshot produced by the snapshot_builder Lambda:

    [date: Date, symbol: Utf8, open, high, low, close, volume: Float64]

Why this is fast
----------------
Polars window functions (``.over("symbol")``) compute per-symbol rolling
indicators and shifts across all symbols at once, using a single vectorized
engine pass with no Python-level loop. A 10k-symbol x 5y frame (~12M rows)
fits comfortably in RAM and scans in well under a second per strategy.

CRITICAL correctness rule
--------------------------
Every operation that crosses row boundaries — ``rolling_mean``,
``rolling_std``, ``shift``, ``diff``, ``ewm_mean`` — MUST be wrapped in
``.over("symbol")``. Without it, one symbol's tail bleeds into the next
symbol's head (e.g. AAPL's last 49 closes contaminate AAL's first SMA-50),
silently corrupting signals. The per-symbol library strategies omit ``.over``
because they assume a single-symbol frame; this module re-implements them
universe-aware.

Timeframes
----------
The snapshot is 1d. Longer timeframes (e.g. 3d "long-term") are derived on the
fly via :func:`resample_long` — N consecutive trading rows aggregated per
symbol — and never persisted.
"""

from __future__ import annotations

import polars as pl

SNAPSHOT_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume"]


# ---------------------------------------------------------------------------
# Timeframe resampling (1d base → N trading-day bars)
# ---------------------------------------------------------------------------

def resample_long(df: pl.DataFrame, n_days: int) -> pl.DataFrame:
    """
    Resample a long-format 1d frame to N-calendar-day bars, per symbol.

    PARITY: this must match ``analytics_core.inputs.resample_ohlcv``, which the
    per-symbol scanner uses. That function calls
    ``group_by_dynamic(time_col, every="Nd")`` (calendar windows anchored to a
    fixed epoch origin, left-labeled — the bar's ``date`` is the window START),
    once per symbol. We reproduce it in a single vectorized pass with
    ``group_by="symbol"``. Using calendar windows (not row buckets) is essential:
    row bucketing drifts whenever a symbol has gaps/holidays, which would make
    the resampled bars — and therefore the signals — diverge from production.
    """
    if n_days <= 1:
        return df.sort(["symbol", "date"]).select(SNAPSHOT_COLUMNS)

    grouped = (
        df.sort(["symbol", "date"])
        .group_by_dynamic(
            "date",
            every=f"{n_days}d",
            group_by="symbol",
        )
        .agg(
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
        )
        .sort(["symbol", "date"])
    )
    return grouped.select(SNAPSHOT_COLUMNS)


# ---------------------------------------------------------------------------
# Golden Cross — vectorized over the full universe
# ---------------------------------------------------------------------------

def golden_cross_signals(
    df: pl.DataFrame,
    fast_period: int = 50,
    slow_period: int = 200,
) -> pl.DataFrame:
    """
    Vectorized Golden Cross across all symbols.

    Setup:   SMA(fast) > SMA(slow)                       (uptrend)
    Trigger: SMA(fast) crosses above SMA(slow)           (golden cross)
    Exit:    death cross OR 5% stop loss anchor

    Adds columns: sma_{fast}, sma_{slow}, setup_valid, signal,
    exit_signal, stop_loss_price. Mirrors
    ``strategies.library.GoldenCrossStrategy`` exactly, but every rolling /
    shift is partitioned ``.over("symbol")``.
    """
    fast_col = f"sma_{fast_period}"
    slow_col = f"sma_{slow_period}"

    df = df.sort(["symbol", "date"]).with_columns(
        [
            pl.col("close").rolling_mean(window_size=fast_period).over("symbol").alias(fast_col),
            pl.col("close").rolling_mean(window_size=slow_period).over("symbol").alias(slow_col),
        ]
    )

    df = df.with_columns(
        [
            (pl.col(fast_col) > pl.col(slow_col)).alias("setup_valid"),
            pl.col(fast_col).shift(1).over("symbol").alias("_fast_prev"),
            pl.col(slow_col).shift(1).over("symbol").alias("_slow_prev"),
        ]
    )

    golden_cross = (pl.col(fast_col) > pl.col(slow_col)) & (pl.col("_fast_prev") <= pl.col("_slow_prev"))
    death_cross = (pl.col(fast_col) < pl.col(slow_col)) & (pl.col("_fast_prev") >= pl.col("_slow_prev"))

    df = df.with_columns(
        [
            pl.when(pl.col("setup_valid") & golden_cross)
            .then(pl.lit("BUY"))
            .otherwise(pl.lit("HOLD"))
            .alias("signal"),
            pl.when(death_cross).then(pl.lit("SELL")).otherwise(None).alias("exit_signal"),
            (pl.col("close") * 0.95).alias("stop_loss_price"),
        ]
    )

    return df.drop(["_fast_prev", "_slow_prev"])


# ---------------------------------------------------------------------------
# Vegas Channel — vectorized over the full universe
# ---------------------------------------------------------------------------

# Required EMAs (mirrors VegasChannelStrategy.required_emas).
_VEGAS_EMAS = [8, 13, 144, 169]
_VEGAS_MIN_CANDLES = 169  # longest EMA period; symbols with fewer bars never set up


def _vegas_obs_window(timeframe: str) -> int:
    """
    Mirror ``VegasChannelStrategy._resolve_obs_window`` for a known timeframe.
    1d->28, 3d->20, 5d->20, 8d->14, 13d->14; unknown N -> base//4 (min 2).
    The per-symbol scanner always passes a 'timeframe', so the None->30 fallback
    never applies here.
    """
    base = 28
    window_dict = {1: base, 3: base - 8, 5: base - 8, 8: base - 14, 13: base - 14}
    tf = str(timeframe).strip().lower()
    days = int(tf[:-1]) if tf.endswith("d") and tf[:-1].isdigit() else None
    if days is None:
        return 30
    return max(2, window_dict.get(days, base // 4))


def _ema(period: int) -> pl.Expr:
    """Per-symbol EMA expression (alpha = 2/(period+1), adjust=False)."""
    return pl.col("close").ewm_mean(alpha=2.0 / (period + 1), adjust=False).over("symbol")


def vegas_channel_signals(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
    """
    Vectorized Vegas Channel across all symbols, for one timeframe frame.

    Faithful port of ``strategies.library.VegasChannelStrategy`` (setup ->
    trigger), with every cross-row op partitioned ``.over("symbol")`` and the
    Python ``map_elements`` flags replaced by vectorized ``is_in`` expressions.
    Emits at least: setup_valid, velocity_status, momentum_signal, signal.
    """
    obs_window = _vegas_obs_window(timeframe)

    df = df.sort(["symbol", "date"])

    # 1) EMAs (per symbol)
    df = df.with_columns([_ema(p).alias(f"ema_{p}") for p in _VEGAS_EMAS])

    # 2) velocity_status — purely row-wise (no .over needed)
    st_max = pl.max_horizontal("ema_8", "ema_13")
    st_min = pl.min_horizontal("ema_8", "ema_13")
    lt_max = pl.max_horizontal("ema_144", "ema_169")
    lt_min = pl.min_horizontal("ema_144", "ema_169")
    df = df.with_columns(
        pl.when(
            (pl.col("close") > pl.col("open"))
            & (pl.col("close") > st_max)
            & (pl.col("close") > lt_max)
            & (st_min > lt_max)
        ).then(pl.lit("velocity_maintained"))
        .when((pl.col("close") < pl.col("ema_13")) & (pl.col("close") > pl.col("ema_169")))
        .then(pl.lit("velocity_weak"))
        .when((pl.col("close") < pl.col("ema_13")) & (pl.col("close") < pl.col("ema_169")))
        .then(pl.lit("velocity_loss"))
        .otherwise(pl.lit("velocity_negotiating"))
        .alias("velocity_status")
    )

    # 3) momentum: rolling counts of loss vs maintain over obs_window (per symbol)
    df = df.with_columns(
        [
            pl.col("velocity_status")
            .is_in(["velocity_loss", "velocity_weak", "velocity_negotiating"])
            .cast(pl.Int32)
            .alias("loss_flag"),
            (pl.col("velocity_status") == "velocity_maintained").cast(pl.Int32).alias("maintain_flag"),
        ]
    )
    df = df.with_columns(
        [
            pl.col("loss_flag").rolling_sum(window_size=obs_window).over("symbol").alias("count_velocity_loss"),
            pl.col("maintain_flag").rolling_sum(window_size=obs_window).over("symbol").alias("count_velocity_maintained"),
        ]
    )

    accel_candle = (lt_max <= st_max) & (st_max < pl.col("open")) & (pl.col("open") < pl.col("close"))
    decel_candle = (pl.col("close") < st_min) & (st_min <= lt_min)
    loss_gt_maintain = pl.col("count_velocity_loss") > pl.col("count_velocity_maintained")

    df = df.with_columns(
        pl.when(loss_gt_maintain & accel_candle).then(pl.lit("accelerated"))
        .when(loss_gt_maintain & decel_candle).then(pl.lit("decelerated"))
        .otherwise(None)
        .alias("momentum_signal")
    )

    # 4) cooldown: drop an alert if the same kind fired in the previous 30 bars (per symbol)
    accel_prev30 = (pl.col("momentum_signal") == "accelerated").cast(pl.Int32).shift(1).rolling_sum(window_size=30).over("symbol")
    decel_prev30 = (pl.col("momentum_signal") == "decelerated").cast(pl.Int32).shift(1).rolling_sum(window_size=30).over("symbol")
    df = df.with_columns(
        pl.when((pl.col("momentum_signal") == "accelerated") & (accel_prev30 > 0)).then(None)
        .when((pl.col("momentum_signal") == "decelerated") & (decel_prev30 > 0)).then(None)
        .otherwise(pl.col("momentum_signal"))
        .alias("momentum_signal")
    )

    # 5) setup + trigger. Symbols with < MIN_CANDLES bars never set up (matches
    #    the strategy's early-return guard).
    enough_history = pl.len().over("symbol") >= _VEGAS_MIN_CANDLES
    setup_valid = enough_history & (
        (pl.col("momentum_signal") == "accelerated") | (pl.col("velocity_status") == "velocity_maintained")
    )
    df = df.with_columns(setup_valid.alias("setup_valid"))

    trigger = (pl.col("momentum_signal") == "accelerated") & (pl.col("open") < pl.col("close"))
    df = df.with_columns(
        pl.when(pl.col("setup_valid") & trigger).then(pl.lit("BUY")).otherwise(pl.lit("HOLD")).alias("signal")
    )
    return df


# ---------------------------------------------------------------------------
# Strategy registry + multi-timeframe scoring (mirrors scanner._score_signal)
# ---------------------------------------------------------------------------

# strategy_name -> (signal_fn, timeframes). signal_fn(frame, timeframe) must add
# 'signal' (BUY/HOLD) and 'setup_valid'. Mirrors DailyScanner.get_strategy_metadata.
STRATEGY_REGISTRY = {
    "golden_cross": (lambda f, tf: golden_cross_signals(f), ["1d", "3d", "5d"]),
    "vegas_channel_short_term": (lambda f, tf: vegas_channel_signals(f, tf), ["1d", "3d", "5d"]),
}

_HIGHER_TF_LOOKBACK = 5  # mirrors higher_tf_buy_lookback_candles in _score_signal


def _timeframe_days(tf: str) -> int:
    tf = str(tf).strip().lower()
    return int(tf[:-1]) if tf.endswith("d") and tf[:-1].isdigit() else 10**9


def score_multi_timeframe(
    base_1d: pl.DataFrame,
    strategy_name: str,
    scan_date,
) -> pl.DataFrame:
    """
    Vectorized equivalent of ``DailyScanner._score_signal`` for the whole universe.

    Rules (identical to the per-symbol scanner):
      * weights per timeframe = position+1 (1d=1, 3d=2, 5d=3); total = sum.
      * ANCHOR (lowest tf, 1d): must emit BUY exactly on scan_date, else the
        symbol is dropped. Its weight always counts toward the score.
      * HIGHER tfs: count their weight if a BUY appears within the last
        ``_HIGHER_TF_LOOKBACK`` signal rows on/before scan_date.
      * confidence = weighted_score / total_weight (clamped 0..1).
      * setup_valid (output) = weighted_setup > 0; trigger_met = True.

    Returns one row per qualifying symbol:
      [symbol, signal='BUY', price, confidence, setup_valid, trigger_met, strategy_name]
    """
    signal_fn, timeframes = STRATEGY_REGISTRY[strategy_name]
    ordered = sorted(timeframes, key=_timeframe_days)
    anchor_tf = ordered[0]
    weights = {tf: float(i + 1) for i, tf in enumerate(timeframes)}
    total_weight = sum(weights.values())

    # Run the strategy per timeframe over the full universe.
    sig_by_tf: dict = {}
    for tf in timeframes:
        frame = base_1d if tf == "1d" else resample_long(base_1d, _timeframe_days(tf))
        sig_by_tf[tf] = signal_fn(frame, tf)

    # ---- anchor: BUY exactly on scan_date ----
    anchor = (
        sig_by_tf[anchor_tf]
        .filter((pl.col("date") == scan_date) & (pl.col("signal") == "BUY"))
        .select(
            "symbol",
            pl.col("close").alias("price"),
            pl.col("setup_valid").fill_null(False).alias("_anchor_setup"),
        )
        .unique(subset=["symbol"], keep="last")
    )
    if anchor.height == 0:
        return _empty_score_frame()

    aw = weights[anchor_tf]
    out = anchor.with_columns(
        [
            pl.lit(aw).alias("_wscore"),
            (pl.when(pl.col("_anchor_setup")).then(aw).otherwise(0.0)).alias("_wsetup"),
        ]
    ).drop("_anchor_setup")

    # ---- higher timeframes: BUY within last N signal rows up to scan_date ----
    for tf in timeframes:
        if tf == anchor_tf:
            continue
        w = weights[tf]
        sig = (
            sig_by_tf[tf]
            .filter(pl.col("signal").is_in(["BUY", "SELL"]) & (pl.col("date") <= scan_date))
            .sort(["symbol", "date"], descending=[False, True])
            .group_by("symbol")
            .head(_HIGHER_TF_LOOKBACK)
        )
        # Most-recent BUY per symbol within the lookback window (if any).
        buys = (
            sig.filter(pl.col("signal") == "BUY")
            .sort(["symbol", "date"], descending=[False, True])
            .group_by("symbol")
            .agg(pl.col("setup_valid").fill_null(False).first().alias(f"_setup_{tf}"))
            .with_columns(pl.lit(True).alias(f"_has_{tf}"))
        )
        out = out.join(buys, on="symbol", how="left")
        out = out.with_columns(
            [
                (pl.col("_wscore") + pl.when(pl.col(f"_has_{tf}").fill_null(False)).then(w).otherwise(0.0)).alias("_wscore"),
                (pl.col("_wsetup") + pl.when(pl.col(f"_setup_{tf}").fill_null(False)).then(w).otherwise(0.0)).alias("_wsetup"),
            ]
        ).drop([f"_has_{tf}", f"_setup_{tf}"])

    out = out.with_columns(
        [
            pl.lit("BUY").alias("signal"),
            (pl.col("_wscore") / total_weight).clip(0.0, 1.0).alias("confidence"),
            (pl.col("_wsetup") > 0).alias("setup_valid"),
            pl.lit(True).alias("trigger_met"),
            pl.lit(strategy_name).alias("strategy_name"),
        ]
    )
    return out.select(
        "symbol", "signal", "price", "confidence", "setup_valid", "trigger_met", "strategy_name"
    ).sort("confidence", descending=True)


def _empty_score_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "symbol": pl.Utf8,
            "signal": pl.Utf8,
            "price": pl.Float64,
            "confidence": pl.Float64,
            "setup_valid": pl.Boolean,
            "trigger_met": pl.Boolean,
            "strategy_name": pl.Utf8,
        }
    )


def latest_buys(df: pl.DataFrame, scan_date=None) -> pl.DataFrame:
    """
    Return the BUY rows. If ``scan_date`` is given, restrict to that date so the
    daily scan only surfaces symbols that triggered today.
    """
    out = df.filter(pl.col("signal") == "BUY")
    if scan_date is not None:
        out = out.filter(pl.col("date") == scan_date)
    return out
