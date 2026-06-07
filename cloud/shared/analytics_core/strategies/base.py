"""
Base Strategy Class

Strategies implement setup → trigger → exit on OHLCV frames. Cross-row
operations must use ``self._w(expr)`` so the same code runs on a single-symbol
frame (backtester) or a multi-symbol long-format frame (scanner).
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import polars as pl


class BaseStrategy(ABC):
    """Base class for all trading strategies (setup → trigger → exit)."""

    def __init__(self, name: str, description: Optional[str] = None):
        self.name = name
        self.description = description
        self._partition_by: Optional[str] = None

    def _w(self, expr: pl.Expr) -> pl.Expr:
        """Scope cross-row expressions to the active partition (e.g. symbol)."""
        if self._partition_by:
            return expr.over(self._partition_by)
        return expr

    @abstractmethod
    def setup(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add boolean column ``setup_valid``."""
        pass

    @abstractmethod
    def trigger(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add ``signal`` column (BUY / SELL / HOLD) when setup is valid."""
        pass

    @abstractmethod
    def exit(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add exit logic columns."""
        pass

    def run(self, df: pl.DataFrame, partition_by: Optional[str] = None) -> pl.DataFrame:
        prev_partition = self._partition_by
        self._partition_by = partition_by
        try:
            df = self.setup(df)
            if 'setup_valid' not in df.columns:
                raise ValueError(f"{self.name}: setup() must add 'setup_valid' column")
            df = self.trigger(df)
            if 'signal' not in df.columns:
                raise ValueError(f"{self.name}: trigger() must add 'signal' column")
            return self.exit(df)
        finally:
            self._partition_by = prev_partition

    def get_signals(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(pl.col('signal').is_in(['BUY', 'SELL']))

    def get_latest_signal(self, df: pl.DataFrame) -> Optional[Dict[str, Any]]:
        signals = self.get_signals(df)
        if signals.height == 0:
            return None
        latest = signals.sort('date', descending=True).head(1)
        return {
            'symbol': latest['symbol'][0] if 'symbol' in latest.columns else None,
            'date': latest['date'][0],
            'signal': latest['signal'][0],
            'price': latest['close'][0] if 'close' in latest.columns else None,
            'setup_valid': latest['setup_valid'][0],
            'trigger_met': latest['signal'][0] != 'HOLD',
        }
