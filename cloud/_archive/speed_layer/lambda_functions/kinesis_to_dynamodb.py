"""
Kinesis → DynamoDB Lambda Function

Purpose: Automatically writes resampled OHLCV data from Kinesis streams to DynamoDB tables
with TTL-based retention.

Triggered by: Kinesis Data Streams (event source mapping)
Input: Kinesis records with OHLCV data
Output: DynamoDB items in interval-specific tables
"""

import json
import base64
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# AWS Region
AWS_REGION = os.environ.get('AWS_REGION', 'ca-west-1')

# Retention policy (days) - matches SPEED_LAYER_REQUIREMENTS.md
RETENTION_DAYS = {
    '1min': 7,
    '5min': 7,
    '15min': 30,
    '30min': 30,
    '1h': 90,
    '2h': 90,
    '4h': 90
}

# Table name mapping: interval -> DynamoDB table name
TABLE_NAME_MAP = {
    '1min': 'speed_layer_ohlcv_1min',
    '5min': 'speed_layer_ohlcv_5min',
    '15min': 'speed_layer_ohlcv_15min',
    '30min': 'speed_layer_ohlcv_30min',
    '1h': 'speed_layer_ohlcv_1h',
    '2h': 'speed_layer_ohlcv_2h',
    '4h': 'speed_layer_ohlcv_4h'
}

# Stream name to interval mapping (for determining which table to write to)
STREAM_TO_INTERVAL = {
    'market-data-1min': '1min',
    'market-data-5min': '5min',
    'market-data-15min': '15min',
    'market-data-30min': '30min',
    'market-data-1hour': '1h',
    'market-data-2hour': '2h',
    'market-data-4hour': '4h'
}


def calculate_ttl(timestamp: datetime, retention_days: int) -> int:
    """
    Calculate TTL timestamp (Unix epoch seconds) for DynamoDB
    
    Args:
        timestamp: Data timestamp
        retention_days: Retention period in days
        
    Returns:
        TTL timestamp (Unix epoch seconds)
    """
    expiration_time = timestamp + timedelta(days=retention_days)
    return int(expiration_time.timestamp())


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse timestamp from various formats
    
    Supports:
    - ISO 8601: "2024-01-15T15:30:00.000Z"
    - Unix milliseconds: 1705332600000
    - Unix seconds: 1705332600
    """
    try:
        # Try ISO 8601 format
        if 'T' in timestamp_str or 'Z' in timestamp_str:
            # Remove 'Z' and parse
            ts_str = timestamp_str.replace('Z', '+00:00')
            return datetime.fromisoformat(ts_str)
        
        # Try Unix timestamp (milliseconds or seconds)
        ts_num = float(timestamp_str)
        if ts_num > 1e10:  # Likely milliseconds
            return datetime.fromtimestamp(ts_num / 1000.0)
        else:  # Likely seconds
            return datetime.fromtimestamp(ts_num)
            
    except Exception as e:
        logger.warning(f"Error parsing timestamp '{timestamp_str}': {e}, using current time")
        return datetime.utcnow()


def format_dynamodb_item(candle: Dict[str, Any], interval: str) -> Dict[str, Any]:
    """
    Convert candle dict to DynamoDB item format
    
    Args:
        candle: OHLCV data dict
        interval: Time interval (1min, 5min, etc.)
        
    Returns:
        DynamoDB item dict
    """
    # Parse timestamp
    timestamp_str = candle.get('window_end') or candle.get('timestamp') or candle.get('window_start')
    if not timestamp_str:
        timestamp = datetime.utcnow()
    else:
        timestamp = parse_timestamp(str(timestamp_str))
    
    # Convert timestamp to Unix milliseconds for DynamoDB sort key
    timestamp_ms = int(timestamp.timestamp() * 1000)
    
    # Calculate TTL
    retention_days = RETENTION_DAYS.get(interval, 7)
    ttl = calculate_ttl(timestamp, retention_days)
    
    # Extract OHLCV values
    open_price = candle.get('open_price') or candle.get('open', 0)
    high_price = candle.get('high_price') or candle.get('high', 0)
    low_price = candle.get('low_price') or candle.get('low', 0)
    close_price = candle.get('close_price') or candle.get('close', 0)
    volume = candle.get('volume', 0)
    
    # Build DynamoDB item
    item = {
        'symbol': {'S': str(candle['symbol'])},
        'timestamp': {'N': str(timestamp_ms)},
        'open': {'N': str(float(open_price))},
        'high': {'N': str(float(high_price))},
        'low': {'N': str(float(low_price))},
        'close': {'N': str(float(close_price))},
        'volume': {'N': str(int(volume))},
        'interval': {'S': interval},
        'ttl': {'N': str(ttl)}
    }
    
    # Add optional fields if present
    if 'trade_count' in candle:
        item['trade_count'] = {'N': str(int(candle['trade_count']))}
    
    if 'vwap' in candle:
        item['vwap'] = {'N': str(float(candle['vwap']))}
    
    if 'processing_time' in candle:
        item['processing_time'] = {'S': str(candle['processing_time'])}
    
    return item


def determine_interval_from_stream(stream_name: str) -> Optional[str]:
    """
    Determine interval from Kinesis stream name
    
    Args:
        stream_name: Kinesis stream name
        
    Returns:
        Interval string (1min, 5min, etc.) or None
    """
    return STREAM_TO_INTERVAL.get(stream_name)


def determine_interval_from_data(candle: Dict[str, Any]) -> Optional[str]:
    """
    Determine interval from candle data
    
    Args:
        candle: OHLCV data dict
        
    Returns:
        Interval string (1min, 5min, etc.) or None
    """
    interval = candle.get('interval_type') or candle.get('interval')
    if interval:
        # Normalize interval format
        interval = interval.lower().replace('hour', 'h').replace('min', 'min')
        return interval
    return None


def process_kinesis_record(record: Dict[str, Any], stream_name: str) -> Dict[str, Any]:
    """
    Process a single Kinesis record and write to DynamoDB
    
    Args:
        record: Kinesis record from event
        stream_name: Name of the Kinesis stream
        
    Returns:
        Result dict with status and details
    """
    try:
        # Decode Kinesis record
        payload = base64.b64decode(record['kinesis']['data'])
        candle = json.loads(payload.decode('utf-8'))
        
        # Determine interval
        interval = determine_interval_from_stream(stream_name) or determine_interval_from_data(candle)
        
        if not interval:
            error_msg = f"Could not determine interval for stream {stream_name} and data {candle}"
            logger.error(error_msg)
            return {'success': False, 'error': error_msg}
        
        # Get table name
        table_name = TABLE_NAME_MAP.get(interval)
        if not table_name:
            error_msg = f"No table mapping for interval: {interval}"
            logger.error(error_msg)
            return {'success': False, 'error': error_msg}
        
        # Format DynamoDB item
        item = format_dynamodb_item(candle, interval)
        
        # Write to DynamoDB
        dynamodb.put_item(
            TableName=table_name,
            Item=item
        )
        
        logger.debug(f"Wrote {candle.get('symbol')} {interval} data to {table_name}")
        
        return {
            'success': True,
            'table': table_name,
            'symbol': candle.get('symbol'),
            'interval': interval
        }
        
    except json.JSONDecodeError as e:
        error_msg = f"JSON decode error: {str(e)}"
        logger.error(error_msg)
        return {'success': False, 'error': error_msg}
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = f"DynamoDB error [{error_code}]: {str(e)}"
        logger.error(error_msg)
        return {'success': False, 'error': error_msg}
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {'success': False, 'error': error_msg}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Kinesis → DynamoDB
    
    Args:
        event: Kinesis event with Records array
        context: Lambda context
        
    Returns:
        Result dict with processing statistics
    """
    logger.info(f"Processing {len(event.get('Records', []))} Kinesis records")
    
    # Get stream name from event source ARN
    event_source_arn = event.get('eventSourceARN', '')
    stream_name = event_source_arn.split('/')[-1] if '/' in event_source_arn else 'unknown'
    
    # Process all records
    results = []
    successful = 0
    failed = 0
    
    for record in event.get('Records', []):
        result = process_kinesis_record(record, stream_name)
        results.append(result)
        
        if result.get('success'):
            successful += 1
        else:
            failed += 1
    
    # Log summary
    logger.info(f"Processed {successful} records successfully, {failed} failed")
    
    # Return result
    return {
        'statusCode': 200 if failed == 0 else 207,  # 207 = Multi-Status (some failed)
        'processed': successful,
        'failed': failed,
        'total': len(results),
        'stream': stream_name
    }
