"""Unit tests for scanner resampling and multi-timeframe scoring."""

from datetime import date, timedelta

import polars as pl

from analytics_core.scanner import resample_long, score_multi_timeframe

_START = date(2026, 1, 1)


def _sample_1d(symbol: str, n_days: int, base_close: float = 100.0) -> pl.DataFrame:
    rows = []
    for i in range(n_days):
        c = base_close + i
        rows.append(
            {
                "date": _START + timedelta(days=i),
                "symbol": symbol,
                "open": c,
                "high": c + 1,
                "low": c - 1,
                "close": c,
                "volume": 1_000.0,
            }
        )
    return pl.DataFrame(rows)


def test_resample_long_aggregates_per_symbol_calendar_window():
    df = pl.concat(
        [
            _sample_1d("AAA", 4, base_close=10.0),
            _sample_1d("BBB", 4, base_close=20.0),
        ]
    )
    out = resample_long(df, 2)
    aaa = out.filter(pl.col("symbol") == "AAA").sort("date")
    assert aaa.height == 2
    assert aaa["open"][0] == 10.0
    assert aaa["close"][0] == 11.0
    assert aaa["high"][0] == 12.0
    assert aaa["low"][0] == 9.0
    assert aaa["volume"][0] == 2_000.0


def test_resample_long_passthrough_for_one_day():
    df = _sample_1d("AAA", 3)
    out = resample_long(df, 1)
    assert out.sort(["symbol", "date"]).to_dicts() == df.sort(["symbol", "date"]).to_dicts()


def test_score_multi_timeframe_empty_when_no_anchor_buy():
    df = pl.concat(
        [
            _sample_1d("AAA", 120, base_close=50.0),
            _sample_1d("BBB", 120, base_close=60.0),
        ]
    )
    out = score_multi_timeframe(df, "golden_cross", _START + timedelta(days=119))
    assert out.is_empty()
