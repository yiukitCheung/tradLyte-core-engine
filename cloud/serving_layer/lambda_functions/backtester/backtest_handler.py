"""
Backtest API Lambda Handler

Accepts POST requests with strategy JSON config and returns backtest results.
Endpoint: POST /api/backtest
"""

import json
import os
import logging
import boto3
from datetime import datetime
from typing import Dict, Any, List
from analytics_core.strategies.builder import CompositeStrategy
from analytics_core.executor import MultiTimeframeExecutor
from analytics_core.backtester import Backtester

try:
    from clients.rds_connection import get_rds_connection_string
except ImportError:  # pragma: no cover - container image layout
    from shared.clients.rds_connection import get_rds_connection_string  # type: ignore
# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
secrets_client = boto3.client('secretsmanager', region_name=os.environ.get('AWS_REGION', 'ca-west-1'))

# ----------------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------------

# Collect unique timeframes from strategy component dicts (JSON payloads).
def _collect_timeframes_from_components(components: Dict[str, Any], base_tf: str) -> List[str]:
    """Collect unique timeframes from strategy component dicts (JSON payloads)."""
    tfs = {base_tf}
    if not isinstance(components, dict):
        return list(tfs)
    for comp in components.values():
        if isinstance(comp, dict) and comp.get("timeframe"):
            tfs.add(str(comp["timeframe"]).strip().lower() or base_tf)
    return sorted(tfs)

# ----------------------------------------------------------------------------
# Lambda handler
# ----------------------------------------------------------------------------
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for backtest API
    
    Expected event body (JSON):
    {
        "strategy_name": "Momentum_Swing",
        "symbol": "AAPL",
        "timeframe": "1d",
        "start_date": "2020-01-01",
        "end_date": "2024-12-31",
        "initial_capital": 10000,
        "components": {
            "setup": { ... },
            "trigger": { ... },
            "exit": { ... }
        }
    }
    
    Returns:
        {
            "statusCode": 200,
            "body": {
                "total_return": 0.25,
                "win_rate": 0.65,
                "max_drawdown": -0.15,
                "sharpe_ratio": 1.2,
                "equity_curve": [...],
                "trades": [...]
            }
        }
    """
    try:
        # Parse request body
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})
        
        # Validate required fields
        required_fields = ['strategy_name', 'symbol', 'components']
        for field in required_fields:
            if field not in body:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'Missing required field: {field}'
                    })
                }
        
        # Extract parameters
        strategy_name = body['strategy_name']
        symbol = str(body['symbol']).strip().upper()
        timeframe = str(body.get('timeframe', '1d')).strip().lower() or '1d'
        start_date_str = body.get('start_date')
        end_date_str = body.get('end_date')
        initial_capital = body.get('initial_capital', 10000.0)
        components = body['components']
        
        # Parse dates
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
        
        # Validate dates
        if not start_date or not end_date:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'start_date and end_date are required'
                })
            }
        # Validate date range
        if start_date > end_date:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'start_date must be on or before end_date'}),
            }
        # Validate date range exceeds maximum of 5 years
        max_days = int(os.environ.get('BACKTEST_MAX_LOOKBACK_DAYS', '1825'))  # ~5 years
        if (end_date - start_date).days > max_days:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Date range exceeds maximum of {max_days} days (~5 years)',
                }),
            }
        
        # Validate initial capital
        if initial_capital <= 0:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'initial_capital must be greater than 0'
                })
            }
        
        # Get RDS connection string
        try:
            rds_connection_string = get_rds_connection_string()
        except Exception as e:
            logger.error(f"Error getting RDS connection: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Failed to connect to database'
                })
            }
        
        # Build strategy from requirements JSON
        strategy_config = {
            'strategy_name': strategy_name,
            'symbol': symbol,
            'timeframe': timeframe,
            'components': components
        }
        
        # Build strategy from requirements JSON
        try:
            strategy = CompositeStrategy.from_requirements_json(strategy_config)
        except Exception as e:
            logger.error(f"Error building strategy: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Invalid strategy configuration: {str(e)}'
                })
            }   
        # Backtestor Logic Starts Here

        # Step 1: Initialize multi-timeframe executor
        executor = MultiTimeframeExecutor(rds_connection_string=rds_connection_string)
        # Step 2: Collect unique timeframes from strategy component dicts (JSON payloads).
        timeframes = _collect_timeframes_from_components(components, timeframe)
        # Step 3: Execute strategy on multi-timeframe data
        try:
            result_df = executor.execute(
                strategy=strategy,
                symbol=symbol,
                timeframes=timeframes,
                start_date=start_date,
                end_date=end_date,
                base_timeframe=timeframe,
            )
        except Exception as e:
            logger.error(f"Error executing strategy: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'Strategy execution failed: {str(e)}'
                })
            }
        
        # Run backtest
        backtester = Backtester(initial_capital=initial_capital)
        
        # Extract position-relative exit parameters from the JSON. The
        # Backtester applies these as OR-composed exits; the JSON layer just
        # collects them. Two authoring shapes are supported and treated
        # symmetrically — a single top-level rule (e.g. just `STOP_LOSS_PCT`)
        # and the `CONDITIONAL_OR_FIXED` wrapper that bundles multiple rules.
        exit_component = components.get('exit', {})
        stop_loss_pct = None
        take_profit_pct = None
        trailing_stop_pct = None
        max_holding_days = None
        stop_loss_anchor = None
        stop_loss_anchor_offset_pct = 0.0

        def _extract_leaf(cond: Dict[str, Any]) -> None:
            """Read one leaf exit rule into the local accumulators above.

            Shared by both authoring shapes so top-level and
            `CONDITIONAL_OR_FIXED.conditions[]` behave identically. Unknown
            types are silently skipped — the vectorized leaves
            (`INDICATOR_CROSS`, `EXPRESSION`) are handled by the strategy
            layer's `_execute_exit_requirements`, not here.
            """
            nonlocal stop_loss_pct, take_profit_pct, trailing_stop_pct
            nonlocal max_holding_days, stop_loss_anchor, stop_loss_anchor_offset_pct
            ctype = cond.get('type')
            if ctype == 'STOP_LOSS_PCT':
                stop_loss_pct = cond.get('value')
            elif ctype == 'TAKE_PROFIT_PCT':
                take_profit_pct = cond.get('value')
            elif ctype == 'TRAILING_STOP_PCT':
                trailing_stop_pct = cond.get('value')
            elif ctype == 'TIME_BASED':
                max_holding_days = cond.get('max_holding_days') or cond.get('value')
            elif ctype == 'STOP_LOSS_ANCHOR':
                stop_loss_anchor = cond.get('anchor')
                stop_loss_anchor_offset_pct = float(cond.get('offset_pct') or 0.0)

        if exit_component.get('type') == 'CONDITIONAL_OR_FIXED':
            for cond in exit_component.get('conditions', []) or []:
                _extract_leaf(cond)
        else:
            # Single top-level rule: extract it the same way so a user can
            # author e.g. `{"type": "TIME_BASED", "max_holding_days": 20}` as
            # a one-rule exit and have it enforced.
            _extract_leaf(exit_component)
        try:
            backtest_result = backtester.run(
                strategy=strategy,
                data=result_df,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                trailing_stop_pct=trailing_stop_pct,
                max_holding_days=max_holding_days,
                stop_loss_anchor=stop_loss_anchor,
                stop_loss_anchor_offset_pct=stop_loss_anchor_offset_pct,
            )
        except Exception as e:
            logger.error(f"Error running backtest: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f'Backtest failed: {str(e)}'
                })
            }
        
        # Return results
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'  # CORS for frontend
            },
            'body': json.dumps(backtest_result.to_dict())
        }
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Internal server error: {str(e)}'
            })
        }
