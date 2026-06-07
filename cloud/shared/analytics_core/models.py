"""
Pydantic Models for Strategy Configuration

Validates JSON strategy configurations from users/API
Supports both legacy 3-step format and new expandable step-based format
"""

from typing import Annotated, Literal, Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field, model_validator


class SetupConfig(BaseModel):
    """Setup (Momentum) Configuration - Step 1: Is the trend valid?"""
    type: Literal[
        'RSI_MOMENTUM',
        'SMA_TREND',
        'MACD_TREND',
        'VOLUME_TREND',
        'NONE'
    ] = Field(..., description="Type of setup/momentum filter")
    
    # RSI Momentum
    min_rsi: Optional[float] = Field(None, ge=0, le=100, description="Minimum RSI value")
    max_rsi: Optional[float] = Field(None, ge=0, le=100, description="Maximum RSI value")
    
    # SMA Trend
    fast_period: Optional[int] = Field(None, gt=0, description="Fast SMA period")
    slow_period: Optional[int] = Field(None, gt=0, description="Slow SMA period")
    direction: Optional[Literal['ABOVE', 'BELOW']] = Field(None, description="Fast above/below slow")
    
    # MACD Trend
    macd_signal: Optional[Literal['BULLISH', 'BEARISH']] = Field(None, description="MACD signal direction")
    
    # Volume Trend
    volume_multiplier: Optional[float] = Field(None, gt=0, description="Volume must be X times average")
    
    @model_validator(mode='after')
    def slow_greater_than_fast(self):
        if (
            self.slow_period
            and self.fast_period
            and self.slow_period <= self.fast_period
        ):
            raise ValueError('slow_period must be greater than fast_period')
        return self


class TriggerConfig(BaseModel):
    """Trigger (Pattern) Configuration - Step 2: Did the entry happen?"""
    type: Literal[
        'CANDLE_PATTERN',
        'PRICE_CROSSOVER',
        'INDICATOR_CROSSOVER',
        'BREAKOUT',
        'REVERSAL'
    ] = Field(..., description="Type of trigger/entry signal")
    
    # Candle Pattern
    pattern: Optional[Literal[
        'ENGULFING_BULLISH',
        'ENGULFING_BEARISH',
        'DOJI',
        'HAMMER',
        'SHOOTING_STAR',
        'MORNING_STAR',
        'EVENING_STAR'
    ]] = Field(None, description="Candle pattern type")
    
    # Price Crossover
    price_level: Optional[float] = Field(None, description="Price level to cross")
    direction: Optional[Literal['ABOVE', 'BELOW']] = Field(None, description="Cross above/below")
    
    # Indicator Crossover
    indicator1: Optional[str] = Field(None, description="First indicator name")
    indicator2: Optional[str] = Field(None, description="Second indicator name")
    crossover_type: Optional[Literal['GOLDEN_CROSS', 'DEATH_CROSS']] = Field(None, description="Crossover type")
    
    # Breakout
    breakout_type: Optional[Literal['BOLLINGER_UPPER', 'BOLLINGER_LOWER', 'RESISTANCE', 'SUPPORT']] = Field(None, description="Breakout type")
    confirmation_bars: Optional[int] = Field(None, ge=1, description="Bars to confirm breakout")
    
    # Reversal
    reversal_type: Optional[Literal['RSI_OVERSOLD', 'RSI_OVERBOUGHT', 'STOCH_OVERSOLD', 'STOCH_OVERBOUGHT']] = Field(None, description="Reversal type")


class ExitConfig(BaseModel):
    """Exit (Management) Configuration - Step 3: When do we sell?"""
    type: Literal[
        'STOP_LOSS',
        'TAKE_PROFIT',
        'TRAILING_STOP',
        'TIME_BASED',
        'INDICATOR_SIGNAL',
        'COMBINED'
    ] = Field(..., description="Type of exit/management rule")
    
    # Stop Loss
    stop_loss_pct: Optional[float] = Field(None, ge=0, le=1, description="Stop loss percentage (e.g., 0.05 = 5%)")
    stop_loss_atr_multiplier: Optional[float] = Field(None, gt=0, description="Stop loss as ATR multiplier")
    
    # Take Profit
    take_profit_pct: Optional[float] = Field(None, ge=0, description="Take profit percentage")
    take_profit_atr_multiplier: Optional[float] = Field(None, gt=0, description="Take profit as ATR multiplier")
    
    # Trailing Stop
    trailing_stop_pct: Optional[float] = Field(None, ge=0, le=1, description="Trailing stop percentage")
    trailing_stop_atr_multiplier: Optional[float] = Field(None, gt=0, description="Trailing stop as ATR multiplier")
    
    # Time Based
    max_holding_days: Optional[int] = Field(None, gt=0, description="Maximum holding period in days")
    
    # Indicator Signal
    exit_indicator: Optional[str] = Field(None, description="Indicator name for exit signal")
    exit_condition: Optional[Literal['CROSSOVER', 'THRESHOLD']] = Field(None, description="Exit condition type")
    
    # Combined (multiple exit rules)
    exit_rules: Optional[List[Dict[str, Any]]] = Field(None, description="Multiple exit rules (OR logic)")


class StrategyConfig(BaseModel):
    """Complete Strategy Configuration"""
    name: str = Field(..., description="Strategy name")
    description: Optional[str] = Field(None, description="Strategy description")
    
    setup: SetupConfig = Field(..., description="Setup (momentum) configuration")
    trigger: TriggerConfig = Field(..., description="Trigger (entry) configuration")
    exit: ExitConfig = Field(..., description="Exit (management) configuration")
    
    # Optional filters
    min_market_cap: Optional[float] = Field(None, description="Minimum market cap filter")
    max_market_cap: Optional[float] = Field(None, description="Maximum market cap filter")
    sectors: Optional[List[str]] = Field(None, description="Allowed sectors")
    exclude_sectors: Optional[List[str]] = Field(None, description="Excluded sectors")
    
    # Performance tracking
    enabled: bool = Field(True, description="Whether strategy is enabled")
    priority: int = Field(0, description="Priority for scanning (higher = first)")


class SignalResult(BaseModel):
    """Result from strategy execution"""
    symbol: str
    date: str
    signal: Literal['BUY', 'SELL', 'HOLD']
    price: float
    setup_valid: bool
    trigger_met: bool
    confidence: Optional[float] = Field(None, ge=0, le=1, description="Signal confidence score")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional strategy metadata")


# ============================================================================
# NEW: Expandable Step-Based Architecture (Requirements JSON Format)
# ============================================================================

class StepConfig(BaseModel):
    """Base class for expandable strategy steps"""
    step_name: str = Field(..., description="Name of the step (e.g., 'setup', 'trigger', 'exit')")
    timeframe: str = Field("1d", description="Candle interval for this step (e.g., '1d', '3d', '5d', '1h')")
    enabled: bool = Field(True, description="Whether this step is enabled")


class IndicatorThresholdConfig(BaseModel):
    """Indicator threshold configuration (for requirements JSON format)"""
    type: Literal['INDICATOR_THRESHOLD'] = 'INDICATOR_THRESHOLD'
    indicator: str = Field(..., description="Indicator name (e.g., 'RSI', 'SMA', 'EMA', 'MACD')")
    params: Dict[str, Any] = Field(default_factory=dict, description="Indicator parameters (e.g., {'period': 14})")
    operator: Literal['>', '<', '>=', '<=', '==', 'CROSS_ABOVE', 'CROSS_BELOW'] = Field(..., description="Comparison operator")
    value: Optional[float] = Field(None, description="Threshold value (not required for CROSS_ABOVE/BELOW)")
    indicator2: Optional[str] = Field(None, description="Second indicator for crossover (required for CROSS_ABOVE/BELOW)")


class CandlePatternConfig(BaseModel):
    """Candle pattern configuration (for requirements JSON format)"""
    type: Literal['CANDLE_PATTERN'] = 'CANDLE_PATTERN'
    pattern: Literal[
        'BULLISH_ENGULFING',
        'BEARISH_ENGULFING',
        'ENGULFING_BULLISH',  # Alias for backward compatibility
        'ENGULFING_BEARISH',  # Alias for backward compatibility
        'HAMMER',
        'SHOOTING_STAR',
        'DOJI',
        'MORNING_STAR',
        'EVENING_STAR',
        'GREEN_CANDLE',  # Simple bullish candle
        'RED_CANDLE'     # Simple bearish candle
    ] = Field(..., description="Candle pattern type")


class PriceCrossoverConfig(BaseModel):
    """Price crossover configuration"""
    type: Literal['PRICE_CROSSOVER'] = 'PRICE_CROSSOVER'
    price_level: Optional[float] = Field(None, description="Price level to cross")
    indicator: Optional[str] = Field(None, description="Indicator to cross (e.g., 'SMA_50')")
    direction: Literal['ABOVE', 'BELOW'] = Field(..., description="Cross above/below")


class IndicatorCrossoverConfig(BaseModel):
    """Indicator crossover configuration"""
    type: Literal['INDICATOR_CROSSOVER'] = 'INDICATOR_CROSSOVER'
    indicator1: str = Field(..., description="First indicator name")
    indicator2: str = Field(..., description="Second indicator name")
    crossover_type: Literal['GOLDEN_CROSS', 'DEATH_CROSS'] = Field(..., description="Crossover type")


class StopLossConfig(BaseModel):
    """Stop loss configuration"""
    type: Literal['STOP_LOSS_PCT', 'STOP_LOSS_ATR'] = Field(..., description="Stop loss type")
    value: float = Field(..., description="Stop loss value (percentage or ATR multiplier)")


class StopLossAnchorConfig(BaseModel):
    """
    Structural stop loss anchored to the entry candle's OHLC.

    For long positions the effective stop price is::

        anchor_value * (1 - offset_pct)

    where ``anchor_value`` is the entry bar's open/high/low/close. Composes
    with ``STOP_LOSS_PCT`` and ``TRAILING_STOP_PCT`` via OR logic — whichever
    stop triggers first wins.

    ``ENTRY_HIGH`` sits at or above the entry close, so using it as a long
    stop tends to trigger immediately on the first down-tick. Allowed for
    completeness but rarely useful on the long side.
    """
    type: Literal['STOP_LOSS_ANCHOR'] = 'STOP_LOSS_ANCHOR'
    anchor: Literal['ENTRY_OPEN', 'ENTRY_HIGH', 'ENTRY_LOW', 'ENTRY_CLOSE'] = Field(
        ...,
        description="Which OHLC component of the entry candle to anchor against",
    )
    offset_pct: Optional[float] = Field(
        0.0,
        ge=0,
        lt=1,
        description="Buffer below the anchor (0.005 = 0.5% below); 0 means anchor exactly",
    )


class TakeProfitConfig(BaseModel):
    """Take profit configuration"""
    type: Literal['TAKE_PROFIT_PCT', 'TAKE_PROFIT_ATR'] = Field(..., description="Take profit type")
    value: float = Field(..., description="Take profit value (percentage or ATR multiplier)")


class IndicatorExitConfig(BaseModel):
    """Indicator-based exit configuration"""
    type: Literal['INDICATOR_CROSS'] = 'INDICATOR_CROSS'
    indicator: str = Field(..., description="Indicator name")
    direction: Literal['UP', 'DOWN'] = Field(..., description="Cross direction")
    value: Optional[float] = Field(None, description="Threshold value (optional)")


class ConditionalOrFixedConfig(BaseModel):
    """Conditional or fixed exit configuration (OR logic)"""
    type: Literal['CONDITIONAL_OR_FIXED'] = 'CONDITIONAL_OR_FIXED'
    conditions: List[Union[StopLossConfig, StopLossAnchorConfig, TakeProfitConfig, IndicatorExitConfig]] = Field(
        ..., 
        description="List of exit conditions (OR logic - any condition triggers exit)"
    )


class SetupComponentConfig(BaseModel):
    """
    Setup component configuration (requirements JSON format).

    Two authoring styles are supported on the same model:

    1. **Legacy flat form** (kept for backward compatibility): set
       ``type='INDICATOR_THRESHOLD'`` and fill ``indicator`` / ``params`` /
       ``operator`` / ``value``. Limited to a single indicator/operator.

    2. **Expression form** (recommended for new code): set
       ``type='EXPRESSION'`` and supply ``expression`` as an arbitrary nested
       boolean tree built from operands (indicators, price fields, constants)
       and operators (comparators, crossovers, AND/OR/NOT). See
       ``ConditionNode``.
    """
    type: Literal['INDICATOR_THRESHOLD', 'EXPRESSION', 'NONE'] = Field(..., description="Setup type")
    timeframe: str = Field("1d", description="Candle interval for setup")
    # Legacy flat form
    indicator: Optional[str] = Field(None, description="Indicator name (legacy flat form)")
    params: Optional[Dict[str, Any]] = Field(None, description="Indicator parameters (legacy)")
    operator: Optional[Literal['>', '<', '>=', '<=', '==', 'CROSS_ABOVE', 'CROSS_BELOW']] = Field(None, description="Comparison operator (legacy)")
    value: Optional[float] = Field(None, description="Threshold value (legacy)")
    indicator2: Optional[str] = Field(None, description="Second indicator for crossover (legacy)")
    # New expression form
    expression: Optional['ConditionNode'] = Field(None, description="Boolean expression tree (new form)")


class TriggerComponentConfig(BaseModel):
    """
    Trigger component configuration (requirements JSON format).

    Supports the same two authoring styles as :class:`SetupComponentConfig`:
    legacy flat fields, or a single recursive ``expression`` evaluated to a
    boolean column that becomes the BUY trigger (with ``signal_value``
    controlling BUY vs SELL).
    """
    type: Literal['CANDLE_PATTERN', 'PRICE_CROSSOVER', 'INDICATOR_CROSSOVER', 'EXPRESSION'] = Field(..., description="Trigger type")
    timeframe: str = Field("1d", description="Candle interval for trigger")
    pattern: Optional[str] = Field(None, description="Candle pattern (for CANDLE_PATTERN type)")
    price_level: Optional[float] = Field(None, description="Price level (for PRICE_CROSSOVER type)")
    indicator: Optional[str] = Field(None, description="Indicator name (for PRICE_CROSSOVER or INDICATOR_CROSSOVER)")
    indicator1: Optional[str] = Field(None, description="First indicator (for INDICATOR_CROSSOVER)")
    indicator2: Optional[str] = Field(None, description="Second indicator (for INDICATOR_CROSSOVER)")
    crossover_type: Optional[Literal['GOLDEN_CROSS', 'DEATH_CROSS']] = Field(None, description="Crossover type")
    direction: Optional[Literal['ABOVE', 'BELOW']] = Field(None, description="Direction (for PRICE_CROSSOVER)")
    # New expression form
    expression: Optional['ConditionNode'] = Field(None, description="Boolean expression tree (new form)")
    signal_value: Optional[Literal['BUY', 'SELL']] = Field('BUY', description="Signal emitted when expression is true")


class ExitComponentConfig(BaseModel):
    """
    Exit component configuration (requirements JSON format).

    Adds an optional ``expression`` for arbitrary boolean exit conditions.
    Position-relative exits (STOP_LOSS_PCT, STOP_LOSS_ANCHOR, TAKE_PROFIT_PCT,
    TRAILING_STOP_PCT, TIME_BASED) continue to be extracted by the backtest
    handler and applied by the Backtester against ``Position.entry_price``,
    ``peak_price``, or the captured entry-candle OHLC.
    """
    type: Literal['CONDITIONAL_OR_FIXED', 'STOP_LOSS_PCT', 'STOP_LOSS_ANCHOR', 'TAKE_PROFIT_PCT', 'TRAILING_STOP_PCT', 'TIME_BASED', 'INDICATOR_CROSS', 'EXPRESSION'] = Field(..., description="Exit type")
    timeframe: str = Field("1d", description="Candle interval for exit")
    conditions: Optional[List[Dict[str, Any]]] = Field(None, description="Exit conditions (for CONDITIONAL_OR_FIXED)")
    value: Optional[float] = Field(None, description="Exit value (for percentage-based exits)")
    indicator: Optional[str] = Field(None, description="Indicator name (for INDICATOR_CROSS)")
    direction: Optional[Literal['UP', 'DOWN']] = Field(None, description="Cross direction (for INDICATOR_CROSS)")
    max_holding_days: Optional[int] = Field(None, description="Max holding days (for TIME_BASED)")
    # STOP_LOSS_ANCHOR fields (entry-candle structural stop)
    anchor: Optional[Literal['ENTRY_OPEN', 'ENTRY_HIGH', 'ENTRY_LOW', 'ENTRY_CLOSE']] = Field(
        None,
        description="Anchor reference (for STOP_LOSS_ANCHOR)",
    )
    offset_pct: Optional[float] = Field(
        None,
        ge=0,
        lt=1,
        description="Buffer below the anchor for STOP_LOSS_ANCHOR (default 0)",
    )
    # New expression form
    expression: Optional['ConditionNode'] = Field(None, description="Boolean expression tree (new form)")


class RequirementsStrategyConfig(BaseModel):
    """Strategy configuration matching requirements JSON format"""
    strategy_name: str = Field(..., description="Strategy name")
    symbol: Optional[str] = Field(None, description="Symbol (optional, for backtesting)")
    timeframe: str = Field("1d", description="Base timeframe for the strategy")
    components: Dict[str, Union[SetupComponentConfig, TriggerComponentConfig, ExitComponentConfig]] = Field(
        ...,
        description="Strategy components (setup, trigger, exit)"
    )
    initial_capital: Optional[float] = Field(None, description="Initial capital for backtesting")
    start_date: Optional[str] = Field(None, description="Start date for backtesting (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date for backtesting (YYYY-MM-DD)")


class ExpandableStrategyConfig(BaseModel):
    """Expandable strategy configuration (supports N steps, not just 3)"""
    name: str = Field(..., description="Strategy name")
    description: Optional[str] = Field(None, description="Strategy description")
    steps: List[StepConfig] = Field(..., min_length=1, description="List of strategy steps (expandable)")
    base_timeframe: str = Field("1d", description="Base timeframe for the strategy")
    
    # Optional filters
    min_market_cap: Optional[float] = Field(None, description="Minimum market cap filter")
    max_market_cap: Optional[float] = Field(None, description="Maximum market cap filter")
    sectors: Optional[List[str]] = Field(None, description="Allowed sectors")
    exclude_sectors: Optional[List[str]] = Field(None, description="Excluded sectors")
    
    # Performance tracking
    enabled: bool = Field(True, description="Whether strategy is enabled")
    priority: int = Field(0, description="Priority for scanning (higher = first)")


# ============================================================================
# Expression-Tree DSL (parametric, recursive condition grammar)
# ============================================================================
#
# A tiny expression DSL backs the new ``EXPRESSION`` component type. Three
# node families:
#
# 1. **Operands** — leaves that produce a Polars column:
#    - ``IndicatorOperand``   {"indicator": "RSI", "params": {"period": 13}}
#    - ``IndicatorOperand``   {"indicator": "MACD", "output": "signal"}
#    - ``PriceOperand``       {"price": "close"}
#    - ``ConstOperand``       {"const": 50}
#
# 2. **Comparators / patterns** — produce a boolean column:
#    - ``CompareNode``        {"op": "CROSS_ABOVE", "left": ..., "right": ...}
#    - ``PatternNode``        {"op": "PATTERN", "pattern": "DOJI"}
#
# 3. **Combinators** — boolean composition:
#    - ``AndNode``            {"op": "AND", "conditions": [...]}
#    - ``OrNode``             {"op": "OR",  "conditions": [...]}
#    - ``NotNode``            {"op": "NOT", "condition": ...}
#
# Pydantic discriminated unions select the right model from the ``op``/
# leaf-key shape. The evaluator (in ``strategies/expression.py``) walks the
# tree, calls the indicator registry to materialise any missing columns, and
# returns a single Polars expression.


class IndicatorOperand(BaseModel):
    """Operand: read a column produced by a registered indicator."""
    indicator: str = Field(..., description="Indicator name registered in INDICATOR_REGISTRY (e.g. RSI, MACD, EMA)")
    params: Optional[Dict[str, Any]] = Field(None, description="Indicator parameter overrides (e.g. {'period': 13})")
    output: Optional[str] = Field(
        None,
        description=(
            "Output role for multi-output indicators (e.g. 'signal' for MACD, "
            "'upper' for BB). Defaults to the indicator's primary output."
        ),
    )


class PriceOperand(BaseModel):
    """Operand: read a raw OHLCV column."""
    price: Literal['open', 'high', 'low', 'close', 'volume'] = Field(..., description="OHLCV column name")


class ConstOperand(BaseModel):
    """Operand: a numeric literal."""
    const: float = Field(..., description="Numeric literal")


Operand = Union[IndicatorOperand, PriceOperand, ConstOperand]


class CompareNode(BaseModel):
    """Boolean comparison or crossover between two operands."""
    op: Literal['GT', 'LT', 'GTE', 'LTE', 'EQ', 'NEQ', 'CROSS_ABOVE', 'CROSS_BELOW'] = Field(..., description="Comparison operator")
    left: Operand = Field(..., description="Left-hand operand")
    right: Operand = Field(..., description="Right-hand operand")


class PatternNode(BaseModel):
    """Boolean output of a candle-pattern detector."""
    op: Literal['PATTERN'] = 'PATTERN'
    pattern: str = Field(..., description="Pattern name registered in PATTERN_REGISTRY (e.g. DOJI, HAMMER)")


class AndNode(BaseModel):
    """Logical AND of N child conditions."""
    op: Literal['AND'] = 'AND'
    conditions: List['ConditionNode'] = Field(..., min_length=1, description="Child conditions (AND-combined)")


class OrNode(BaseModel):
    """Logical OR of N child conditions."""
    op: Literal['OR'] = 'OR'
    conditions: List['ConditionNode'] = Field(..., min_length=1, description="Child conditions (OR-combined)")


class NotNode(BaseModel):
    """Logical NOT of a single child condition."""
    op: Literal['NOT'] = 'NOT'
    condition: 'ConditionNode' = Field(..., description="Child condition to negate")


# Discriminated union so Pydantic picks the right model based on ``op``.
ConditionNode = Annotated[
    Union[CompareNode, PatternNode, AndNode, OrNode, NotNode],
    Field(discriminator='op'),
]


# Recursive forward-ref resolution. Pydantic v2 needs an explicit rebuild on
# any model that referenced ``ConditionNode`` by string before it existed.
AndNode.model_rebuild()
OrNode.model_rebuild()
NotNode.model_rebuild()
SetupComponentConfig.model_rebuild()
TriggerComponentConfig.model_rebuild()
ExitComponentConfig.model_rebuild()
