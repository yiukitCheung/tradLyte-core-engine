"""
AWS Lambda function for WebSocket API disconnection
Handles WebSocket disconnections and cleanup

Purpose:
- Handle $disconnect route (when client disconnects)
- Remove connection from DynamoDB
- Clean up subscriptions
"""

import json
import boto3
import logging
import os
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
connections_table = dynamodb.Table(os.environ.get('CONNECTIONS_TABLE', 'websocket_connections'))


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Lambda handler for WebSocket $disconnect route
    
    Event structure:
    {
        'requestContext': {
            'connectionId': 'abc123',
            'routeKey': '$disconnect',
            'eventType': 'DISCONNECT'
        }
    }
    """
    try:
        connection_id = event['requestContext']['connectionId']
        
        logger.info(f"WebSocket disconnection: {connection_id}")
        
        # Remove connection from DynamoDB
        connections_table.delete_item(
            Key={'connection_id': connection_id}
        )
        
        logger.info(f"✅ Connection {connection_id} removed from DynamoDB")
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Disconnected successfully'})
        }
        
    except Exception as e:
        logger.error(f"Error handling disconnection: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

