"""
AWS Lambda function for WebSocket API connection management
Handles WebSocket connections, disconnections, and subscriptions

Purpose:
- Handle $connect route (when client connects)
- Store connection ID in DynamoDB
- Return connection status
"""

import json
import boto3
import logging
import os
from datetime import datetime
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
connections_table = dynamodb.Table(os.environ.get('CONNECTIONS_TABLE', 'websocket_connections'))


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Lambda handler for WebSocket $connect route
    
    Event structure:
    {
        'requestContext': {
            'connectionId': 'abc123',
            'routeKey': '$connect',
            'eventType': 'CONNECT'
        }
    }
    """
    try:
        connection_id = event['requestContext']['connectionId']
        
        logger.info(f"New WebSocket connection: {connection_id}")
        
        # Store connection in DynamoDB
        connections_table.put_item(
            Item={
                'connection_id': connection_id,
                'connected_at': int(datetime.utcnow().timestamp()),
                'subscriptions': [],  # Initially no subscriptions
                'user_id': None,  # Set when authenticated
                'status': 'connected'
            }
        )
        
        logger.info(f"✅ Connection {connection_id} stored in DynamoDB")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Connected successfully',
                'connection_id': connection_id
            })
        }
        
    except Exception as e:
        logger.error(f"Error handling connection: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

