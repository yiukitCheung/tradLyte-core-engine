"""
Kinesis client for Speed Layer
Simplified async client for streaming data to Kinesis
"""

import boto3
import json
import logging
import asyncio
import os
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class KinesisClient:
    """Async Kinesis client for Speed Layer"""
    
    def __init__(self, stream_name: str, region_name: str = 'ca-west-1'):
        """
        Initialize Kinesis client
        
        Args:
            stream_name: Name of the Kinesis stream
            region_name: AWS region name
        """
        self.stream_name = stream_name
        self.region_name = region_name
        
        # Get endpoint URL from environment (for LocalStack testing)
        endpoint_url = None
        if 'AWS_ENDPOINT_URL' in os.environ:
            endpoint_url = os.environ['AWS_ENDPOINT_URL']
            logger.info(f"Using custom endpoint: {endpoint_url}")
        
        # Create boto3 client (synchronous)
        self.kinesis_client = boto3.client(
            'kinesis',
            region_name=region_name,
            endpoint_url=endpoint_url
        )
        
        logger.info(f"Kinesis client initialized for stream: {stream_name}")
    
    async def put_record(self, data: Dict[str, Any], partition_key: str) -> bool:
        """
        Put a single record to Kinesis stream (async wrapper)
        
        Args:
            data: Dictionary to send as record data
            partition_key: Partition key for the record
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Run synchronous boto3 call in executor to make it async
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.kinesis_client.put_record(
                    StreamName=self.stream_name,
                    Data=json.dumps(data).encode('utf-8'),
                    PartitionKey=partition_key
                )
            )
            
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"Kinesis ClientError [{error_code}]: {str(e)}")
            return False
        except BotoCoreError as e:
            logger.error(f"Kinesis BotoCoreError: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error putting record to Kinesis: {str(e)}")
            return False
    
    async def close(self):
        """Close Kinesis client (no-op for boto3 clients)"""
        # boto3 clients don't need explicit closing
        pass
