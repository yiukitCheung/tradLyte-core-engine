"""
AWS Lambda function for real-time signal generation
Consumes Kinesis Analytics output and evaluates user alert conditions

Purpose:
- Process resampled OHLCV data from Kinesis Analytics
- Evaluate price thresholds (e.g., "alert if AAPL > $200")
- Evaluate technical indicators (RSI, MACD, etc.)
- Publish alerts to SNS for frontend delivery
- Cache latest signal status in Redis
"""

import json
import boto3
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
alerts_table = dynamodb.Table(os.environ.get('ALERTS_CONFIG_TABLE', 'alert_configurations'))


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Lambda handler for signal generation
    
    Event source: Kinesis Analytics output stream
    
    Event format:
    {
        'Records': [
            {
                'kinesis': {
                    'data': base64_encoded_data
                },
                'eventID': '...',
                'eventSource': 'aws:kinesis'
            }
        ]
    }
    """
    try:
        logger.info(f"Processing {len(event['Records'])} records from Kinesis")
        
        processed_count = 0
        alert_count = 0
        
        for record in event['Records']:
            # Decode Kinesis data
            payload = json.loads(
                record['kinesis']['data'],
                parse_float=Decimal
            )
            
            logger.info(f"Processing OHLCV data: {payload.get('symbol', 'UNKNOWN')} "
                       f"at {payload.get('timestamp', 'N/A')}")
            
            # Extract OHLCV data
            ohlcv_data = {
                'symbol': payload['symbol'],
                'timestamp': payload['timestamp'],
                'open': payload['open'],
                'high': payload['high'],
                'low': payload['low'],
                'close': payload['close'],
                'volume': payload['volume'],
                'interval': payload.get('interval', '5m')
            }
            
            # Get active alerts for this symbol
            active_alerts = get_active_alerts(ohlcv_data['symbol'])
            
            # Evaluate each alert condition
            for alert in active_alerts:
                if evaluate_alert_condition(alert, ohlcv_data):
                    # Alert condition met - publish notification
                    publish_alert(alert, ohlcv_data)
                    alert_count += 1
            
            processed_count += 1
        
        logger.info(f"✅ Processed {processed_count} records, triggered {alert_count} alerts")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed': processed_count,
                'alerts_triggered': alert_count
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing Kinesis records: {str(e)}")
        raise


def get_active_alerts(symbol: str) -> List[Dict[str, Any]]:
    """
    Get active alert configurations for a symbol from DynamoDB
    
    Alert configuration structure:
    {
        'alert_id': 'uuid',
        'user_id': 'user123',
        'symbol': 'AAPL',
        'condition_type': 'price_threshold',  # or 'indicator', 'fundamental'
        'condition': {
            'operator': '>',
            'threshold': 200.00
        },
        'notification_method': 'sns',  # or 'websocket', 'email'
        'enabled': True
    }
    """
    try:
        response = alerts_table.query(
            IndexName='SymbolIndex',
            KeyConditionExpression='symbol = :symbol',
            FilterExpression='enabled = :enabled',
            ExpressionAttributeValues={
                ':symbol': symbol,
                ':enabled': True
            }
        )
        
        alerts = response.get('Items', [])
        logger.info(f"Found {len(alerts)} active alerts for {symbol}")
        return alerts
        
    except Exception as e:
        logger.error(f"Error fetching alerts for {symbol}: {str(e)}")
        return []


def evaluate_alert_condition(alert: Dict[str, Any], ohlcv_data: Dict[str, Any]) -> bool:
    """
    Evaluate if alert condition is met based on OHLCV data
    
    Supported condition types:
    1. price_threshold: Simple price comparisons (>, <, >=, <=, ==)
    2. price_change: Percentage change thresholds
    3. indicator: Technical indicators (RSI, MACD, etc.) - placeholder
    4. fundamental: Fundamental metrics - placeholder
    """
    try:
        condition_type = alert.get('condition_type', 'price_threshold')
        condition = alert.get('condition', {})
        
        if condition_type == 'price_threshold':
            return evaluate_price_threshold(condition, ohlcv_data)
        
        elif condition_type == 'price_change':
            return evaluate_price_change(condition, ohlcv_data)
        
        elif condition_type == 'indicator':
            # Placeholder for technical indicator evaluation
            logger.warning(f"Indicator evaluation not yet implemented: {condition}")
            return False
        
        elif condition_type == 'fundamental':
            # Placeholder for fundamental metric evaluation
            logger.warning(f"Fundamental evaluation not yet implemented: {condition}")
            return False
        
        else:
            logger.warning(f"Unknown condition type: {condition_type}")
            return False
            
    except Exception as e:
        logger.error(f"Error evaluating alert condition: {str(e)}")
        return False


def evaluate_price_threshold(condition: Dict[str, Any], ohlcv_data: Dict[str, Any]) -> bool:
    """
    Evaluate simple price threshold conditions
    
    Examples:
    - {"operator": ">", "threshold": 200, "price_type": "close"}
    - {"operator": "<", "threshold": 150, "price_type": "low"}
    """
    operator = condition.get('operator', '>')
    threshold = Decimal(str(condition.get('threshold', 0)))
    price_type = condition.get('price_type', 'close')  # open, high, low, close
    
    current_price = Decimal(str(ohlcv_data.get(price_type, 0)))
    
    # Evaluate condition
    if operator == '>':
        return current_price > threshold
    elif operator == '>=':
        return current_price >= threshold
    elif operator == '<':
        return current_price < threshold
    elif operator == '<=':
        return current_price <= threshold
    elif operator == '==':
        return current_price == threshold
    else:
        logger.warning(f"Unknown operator: {operator}")
        return False


def evaluate_price_change(condition: Dict[str, Any], ohlcv_data: Dict[str, Any]) -> bool:
    """
    Evaluate price change percentage conditions
    
    Example:
    - {"operator": ">", "change_percent": 5.0}  # Alert if price up > 5%
    - {"operator": "<", "change_percent": -3.0}  # Alert if price down > 3%
    
    Note: This requires comparing with previous close price
    TODO: Store previous close in DynamoDB or Redis for comparison
    """
    # Placeholder - needs previous price data
    logger.warning("Price change evaluation requires previous price data (not yet implemented)")
    return False


def publish_alert(alert: Dict[str, Any], ohlcv_data: Dict[str, Any]):
    """
    Publish alert notification to SNS topic
    
    SNS message format:
    {
        'alert_id': '...',
        'user_id': '...',
        'symbol': 'AAPL',
        'current_price': 205.50,
        'threshold': 200.00,
        'timestamp': '2025-10-18T12:00:00Z',
        'message': 'AAPL price ($205.50) exceeded threshold ($200.00)'
    }
    """
    try:
        sns_topic_arn = os.environ.get('ALERTS_SNS_TOPIC_ARN')
        
        if not sns_topic_arn:
            logger.error("ALERTS_SNS_TOPIC_ARN environment variable not set")
            return
        
        # Build alert message
        symbol = ohlcv_data['symbol']
        current_price = float(ohlcv_data['close'])
        threshold = float(alert['condition'].get('threshold', 0))
        operator = alert['condition'].get('operator', '>')
        
        message = {
            'alert_id': alert['alert_id'],
            'user_id': alert['user_id'],
            'symbol': symbol,
            'current_price': current_price,
            'threshold': threshold,
            'operator': operator,
            'timestamp': ohlcv_data['timestamp'],
            'message': f"{symbol} price (${current_price:.2f}) {operator} threshold (${threshold:.2f})"
        }
        
        # Publish to SNS
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=f"Price Alert: {symbol}",
            Message=json.dumps(message, default=str),
            MessageAttributes={
                'user_id': {'DataType': 'String', 'StringValue': alert['user_id']},
                'symbol': {'DataType': 'String', 'StringValue': symbol},
                'alert_type': {'DataType': 'String', 'StringValue': 'price_threshold'}
            }
        )
        
        logger.info(f"✅ Published alert for {symbol} to SNS: {response['MessageId']}")
        
        # Cache alert status in Redis (optional - for de-duplication)
        cache_alert_status(alert, ohlcv_data)
        
    except Exception as e:
        logger.error(f"Error publishing alert to SNS: {str(e)}")


def cache_alert_status(alert: Dict[str, Any], ohlcv_data: Dict[str, Any]):
    """
    Cache alert status in Redis to prevent duplicate notifications
    
    Cache key: alert_status:{alert_id}
    TTL: 5 minutes (configurable)
    
    Value: {
        'last_triggered': timestamp,
        'current_price': price,
        'status': 'triggered'
    }
    
    TODO: Implement Redis client
    """
    # Placeholder for Redis caching
    logger.info(f"Cache alert status for alert_id={alert['alert_id']} (Redis not yet configured)")
    pass


# Extension: Technical Indicator Evaluation (Placeholder)
def evaluate_indicator_rsi(ohlcv_data: Dict[str, Any], threshold: float, operator: str) -> bool:
    """
    Evaluate RSI (Relative Strength Index) condition
    
    Example: RSI < 30 (oversold)
    
    TODO: Calculate RSI from historical data
    Requires: Last 14 periods of close prices
    """
    logger.warning("RSI evaluation not yet implemented")
    return False


def evaluate_indicator_macd(ohlcv_data: Dict[str, Any]) -> bool:
    """
    Evaluate MACD (Moving Average Convergence Divergence) crossover
    
    TODO: Calculate MACD from historical data
    Requires: 12-period EMA, 26-period EMA, 9-period signal line
    """
    logger.warning("MACD evaluation not yet implemented")
    return False


# Extension: Fundamental Metrics Evaluation (Placeholder)
def evaluate_fundamental_metric(symbol: str, metric: str, threshold: float, operator: str) -> bool:
    """
    Evaluate fundamental metrics (P/E ratio, market cap, etc.)
    
    Examples:
    - P/E ratio > 25
    - Market cap < 1B
    - Revenue growth > 20%
    
    TODO: Integrate with fundamental data source (e.g., Financial Modeling Prep API)
    """
    logger.warning("Fundamental metrics evaluation not yet implemented")
    return False

