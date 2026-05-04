"""Backtest API route."""

import os
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, HTTPException

from serving_api.db import load_rds_secret
from shared.analytics_core.backtester import Backtester
from shared.analytics_core.executor import MultiTimeframeExecutor
from shared.analytics_core.strategies.builder import CompositeStrategy

router = APIRouter(prefix="/backtest", tags=["backtest"])


def _collect_timeframes_from_components(components: Dict[str, Any], base_tf: str) -> List[str]:
    """Collect unique timeframes from setup/trigger/exit component configs."""
    timeframes = {base_tf}
    if not isinstance(components, dict):
        return sorted(timeframes)
    for comp in components.values():
        if isinstance(comp, dict) and comp.get("timeframe"):
            tf = str(comp["timeframe"]).strip().lower()
            if tf:
                timeframes.add(tf)
    return sorted(timeframes)


def _extract_exit_limits(components: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """Extract stop loss / take profit percentages from requirements-style exit config."""
    exit_component = components.get("exit", {}) if isinstance(components, dict) else {}
    stop_loss_pct: Optional[float] = None
    take_profit_pct: Optional[float] = None

    if not isinstance(exit_component, dict):
        return stop_loss_pct, take_profit_pct

    exit_type = exit_component.get("type")
    if exit_type == "CONDITIONAL_OR_FIXED":
        for cond in exit_component.get("conditions", []):
            if not isinstance(cond, dict):
                continue
            if cond.get("type") == "STOP_LOSS_PCT":
                stop_loss_pct = cond.get("value")
            elif cond.get("type") == "TAKE_PROFIT_PCT":
                take_profit_pct = cond.get("value")
    elif exit_type == "STOP_LOSS_PCT":
        stop_loss_pct = exit_component.get("value")
    elif exit_type == "TAKE_PROFIT_PCT":
        take_profit_pct = exit_component.get("value")

    return stop_loss_pct, take_profit_pct


def _build_rds_connection_string() -> str:
    """Build a safe PostgreSQL URI from Secrets Manager credentials."""
    cfg = load_rds_secret()
    user = quote_plus(str(cfg["username"]))
    password = quote_plus(str(cfg["password"]))
    sslmode = os.environ.get("RDS_SSL_MODE", "require")
    return (
        f"postgresql://{user}:{password}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        f"?sslmode={sslmode}"
    )


@router.post("")
def run_backtest(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run a single-symbol backtest from frontend-provided strategy JSON."""
    required_fields = ["strategy_name", "symbol", "components", "start_date", "end_date"]
    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required fields: {', '.join(missing)}")

    strategy_name = str(payload["strategy_name"]).strip()
    symbol = str(payload["symbol"]).strip().upper()
    timeframe = str(payload.get("timeframe", "1d")).strip().lower() or "1d"
    components = payload.get("components")
    if not strategy_name:
        raise HTTPException(status_code=400, detail="strategy_name must not be empty")
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol must not be empty")
    if not isinstance(components, dict) or not components:
        raise HTTPException(status_code=400, detail="components must be a non-empty object")

    try:
        start_date = date.fromisoformat(str(payload["start_date"]))
        end_date = date.fromisoformat(str(payload["end_date"]))
    except ValueError:
        raise HTTPException(status_code=400, detail="start_date/end_date must be YYYY-MM-DD")

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be on or before end_date")

    max_days = int(os.environ.get("BACKTEST_MAX_LOOKBACK_DAYS", "1825"))
    if (end_date - start_date).days > max_days:
        raise HTTPException(
            status_code=400,
            detail=f"Date range exceeds maximum of {max_days} days (~5 years)",
        )

    initial_capital_raw = payload.get("initial_capital", 10000.0)
    try:
        initial_capital = float(initial_capital_raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="initial_capital must be numeric")
    if initial_capital <= 0:
        raise HTTPException(status_code=400, detail="initial_capital must be > 0")

    strategy_config = {
        "strategy_name": strategy_name,
        "symbol": symbol,
        "timeframe": timeframe,
        "components": components,
    }

    try:
        strategy = CompositeStrategy.from_requirements_json(strategy_config)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid strategy configuration: {exc}")

    try:
        executor = MultiTimeframeExecutor(rds_connection_string=_build_rds_connection_string())
        result_df = executor.execute(
            strategy=strategy,
            symbol=symbol,
            timeframes=_collect_timeframes_from_components(components, timeframe),
            start_date=start_date,
            end_date=end_date,
            base_timeframe=timeframe,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Strategy execution failed: {exc}")

    stop_loss_pct, take_profit_pct = _extract_exit_limits(components)

    try:
        backtest_result = Backtester(initial_capital=initial_capital).run(
            strategy=strategy,
            data=result_df,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Backtest failed: {exc}")

    return {
        "data": backtest_result.to_dict(),
        "meta": {
            "symbol": symbol,
            "strategy_name": strategy_name,
            "timeframe": timeframe,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    }
