"""
Multi-Timeframe Strategy Executor

Handles loading data from different timeframes and aligning signals
for multi-timeframe strategy execution.
"""

import polars as pl
from typing import Dict, List, Optional
from datetime import date
from .inputs import (
    load_ohlcv,
    build_multi_timeframe_from_batch_1d,
    load_ohlcv_multi_timeframe,
    )
from .strategies.base import BaseStrategy
from .indicators.technicals import calculate_all_indicators
from .indicators.patterns import detect_all_patterns


# Columns that represent a one-bar *event* rather than a persistent *state*.
# Event columns must NOT be forward-filled when aligning from a higher
# timeframe down to the base timeframe — otherwise a BUY emitted on a 3d bar
# would replicate onto each underlying 1d bar in the same 3d window. State
# columns (setup_valid, indicator values, etc.) are still forward-filled.
_EVENT_COLUMNS = frozenset({"signal", "exit_signal"})
_EVENT_DEFAULTS: Dict[str, object] = {
    "signal": "HOLD",
    "exit_signal": None,
}


class MultiTimeframeExecutor:
    """
    Executes strategies across multiple timeframes
    
    Handles:
    - Loading data for different timeframes (1d, 3d, 5d, etc.)
    - Executing strategy steps on appropriate timeframes
    - Aligning higher timeframe signals to base timeframe
    """
    
    def __init__(self, rds_connection_string: str):
        """
        Initialize executor with RDS connection. All timeframes load from RDS (1d).
        """
        self.rds_connection_string = rds_connection_string

    def load(
        self,
        symbols: List[str],
        timeframes: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Dict[str, pl.DataFrame]]:
        """
        Load OHLCV for symbols at multiple timeframes.

        Args:
            symbols: List of symbols (e.g. ['AAPL', 'MSFT'])
            timeframes: e.g. ['1d', '3d', '5d']
            start_date: Start date
            end_date: End date

        Returns:
            data_by_symbol[symbol][timeframe] -> DataFrame
        """
        if not self.rds_connection_string:
            raise ValueError("rds_connection_string is required.")
        if not symbols or not timeframes:
            return {}
        batch_1d = load_ohlcv(
            symbols=symbols,
            connection_string=self.rds_connection_string,
            start_date=start_date,
            end_date=end_date,
        )
        if batch_1d.is_empty():
            return {}
        # Build multi-timeframe data by symbol
        # This return a dictionary of symbols and their corresponding timeframes in polars dataframe
        data_by_symbol = build_multi_timeframe_from_batch_1d(batch_1d, timeframes)
        # Ensure each symbol's dfs are sorted by date
        for sym in data_by_symbol:
            for tf in data_by_symbol[sym]:
                df = data_by_symbol[sym][tf]
                if "date" in df.columns and df.height > 0:
                    data_by_symbol[sym][tf] = df.sort("date")
        return data_by_symbol
    
    def prepare_dataframe(self, df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        """
        Prepare dataframe by calculating indicators and patterns
        
        Args:
            df: OHLCV DataFrame
            timeframe: Timeframe string (for logging)
            
        Returns:
            DataFrame with indicators and patterns added
        """
        if df.height == 0:
            return df
        
        # Calculate technical indicators
        df = calculate_all_indicators(df)
        
        # Detect candle patterns
        df = detect_all_patterns(df)

        # Keep explicit timeframe context for timeframe-aware strategy logic.
        df = df.with_columns(pl.lit(timeframe).alias("timeframe"))
        
        return df
    
    def align_timeframe_signals(
        self,
        base_df: pl.DataFrame,
        higher_timeframe_df: pl.DataFrame,
        higher_timeframe: str,
        signal_column: str = 'setup_valid'
    ) -> pl.DataFrame:
        """
        Align higher-timeframe column values onto the base timeframe.

        Two behaviours depending on column semantics:

        - **State columns** (default, e.g. ``setup_valid``, indicator values):
          forward-filled via ``join_asof(strategy='backward')`` so the most
          recent higher-TF value applies to every base-TF bar within its
          period.
        - **Event columns** (``signal``, ``exit_signal``): fire **only** on
          the base-TF bar whose date matches the higher-TF bar's date.
          Other base-TF bars receive the column's neutral default
          (``HOLD`` / ``None``). This avoids duplicating a single BUY/SELL
          event across every underlying bar in the higher-TF window.

        Example: a BUY emitted on the 3d bar dated 2026-03-18 attaches only to
        the 1d row for 2026-03-18, not to the 1d rows for 2026-03-19/20.

        Args:
            base_df: Base timeframe DataFrame (e.g., 1d).
            higher_timeframe_df: Higher timeframe DataFrame (e.g., 3d).
            higher_timeframe: Higher timeframe string (e.g., '3d').
            signal_column: Column name to align (default: 'setup_valid').

        Returns:
            Base DataFrame with the aligned column added/updated.
        """
        if signal_column not in higher_timeframe_df.columns:
            return base_df

        htf = (
            higher_timeframe_df
            .select(['date', signal_column])
            .sort('date')
            .rename({'date': '_htf_date'})
        )

        base_with_signals = base_df.sort('date').join_asof(
            htf,
            left_on='date',
            right_on='_htf_date',
            strategy='backward',
        )

        if signal_column in _EVENT_COLUMNS:
            default_value = _EVENT_DEFAULTS.get(signal_column)
            default_lit = pl.lit(default_value) if default_value is not None else pl.lit(None)
            base_with_signals = base_with_signals.with_columns(
                pl.when(pl.col('date') == pl.col('_htf_date'))
                .then(pl.col(signal_column))
                .otherwise(default_lit)
                .alias(signal_column)
            )

        return base_with_signals.drop('_htf_date')
    
    def run(
        self,
        strategy: BaseStrategy,
        data_by_timeframe: Dict[str, pl.DataFrame],
        base_timeframe: str = '1d'
    ) -> pl.DataFrame:
        """
        Execute strategy on pre-loaded multi-timeframe data.

        Args:
            strategy: Strategy instance
            data_by_timeframe: {timeframe: DataFrame}
            base_timeframe: Base timeframe (default '1d')

        Returns:
            DataFrame with strategy signals
        """
        if base_timeframe not in data_by_timeframe:
            raise ValueError(f"Base timeframe {base_timeframe} not found in data")
        
        base_df = data_by_timeframe[base_timeframe].clone()
        base_df = self.prepare_dataframe(base_df, base_timeframe)
        return strategy.run(base_df)
    
    def execute(
        self,
        strategy: BaseStrategy,
        symbol: str,
        timeframes: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        base_timeframe: str = "1d",
    ) -> pl.DataFrame:
        """
        Load data and execute strategy for a single symbol.

        Args:
            strategy: Strategy instance
            symbol: Stock symbol
            timeframes: e.g. ['1d', '3d']
            start_date: Start date
            end_date: End date
            base_timeframe: Base timeframe (default '1d')

        Returns:
            DataFrame with strategy signals
        """
        if not self.rds_connection_string:
            raise ValueError("rds_connection_string is required.")

        # Load data from RDS
        multi_df = load_ohlcv_multi_timeframe(
            symbol=symbol,
            timeframe=timeframes,
            connection_string=self.rds_connection_string,
            start_date=start_date,
            end_date=end_date,
        )
        if multi_df.is_empty():
            raise ValueError(f"No data loaded for {symbol}")

        data_by_timeframe: Dict[str, pl.DataFrame] = {}
        # Split data by timeframe
        if "timeframe" in multi_df.columns:
            for tf in timeframes:
                tf_df = multi_df.filter(pl.col("timeframe") == tf).drop("timeframe")
                if not tf_df.is_empty():
                    data_by_timeframe[tf] = tf_df.sort("date")
        else:
            # Single-timeframe response
            tf = timeframes[0] if timeframes else base_timeframe
            data_by_timeframe[tf] = multi_df.sort("date")

        # Run strategy on data
        if not data_by_timeframe:
            raise ValueError(f"No data loaded for {symbol}")

        return self.run(strategy, data_by_timeframe, base_timeframe)

    # Backward-compatible alias
    execute_strategy = execute
    execute_strategy_multi_timeframe = run
    load_batch_multi_timeframe_data = load
