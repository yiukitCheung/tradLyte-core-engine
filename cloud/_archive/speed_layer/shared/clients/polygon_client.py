"""
Polygon.io (Massive) client for Speed Layer
Simplified version for market status checks only
"""

import logging
from typing import Dict, Any
from massive import RESTClient

logger = logging.getLogger(__name__)


class PolygonClient:
    """
    Polygon.io API client for Speed Layer (market status only)
    """
    
    def __init__(self, api_key: str):
        """
        Initialize Polygon.io client
        
        Args:
            api_key: Polygon.io API key
        """
        self.api_key = api_key
        self.client = RESTClient(api_key=api_key)
        logger.debug("PolygonClient initialized")
    
    def get_market_status(self) -> Dict[str, Any]:
        """
        Get current market status
        Returns a dictionary containing market status information
        """
        try:
            result = self.client.get_market_status()
            
            # Parse the response into a dictionary
            market_status = {
                'after_hours': result.after_hours,
                'currencies': {
                    'crypto': result.currencies.crypto,
                    'fx': result.currencies.fx
                },
                'early_hours': result.early_hours,
                'exchanges': {
                    'nasdaq': result.exchanges.nasdaq,
                    'nyse': result.exchanges.nyse,
                    'otc': result.exchanges.otc
                },
                'market': result.market,
                'server_time': result.server_time
            }
            
            return market_status
            
        except Exception as e:
            logger.error(f"Error getting market status: {str(e)}")
            raise
