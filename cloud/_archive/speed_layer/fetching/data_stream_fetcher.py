"""
ECS Fargate WebSocket Service for Speed Layer

This service runs 24/7 during market hours and provides:
1. Persistent Massive (formerly Polygon.io) WebSocket connection (no 15-min timeout)
2. Real-time tick data ingestion (15-minute delayed data for testing)
3. Feeds data to Kinesis for signal processing
4. Uses official Massive WebSocket client

Runs on ECS Fargate for continuous operation.
"""

import os
import json
import logging
import asyncio
import signal
import sys
import boto3
from datetime import datetime
from typing import List

# Massive (formerly Polygon) WebSocket Client (official)
from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage
from massive.websocket.models.common import Feed

# HTTP server for health checks
from aiohttp import web
from aiohttp.web_runner import GracefulExit

# Import speed_layer shared utilities (decoupled from main shared directory)
# Add speed_layer directory to path for shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))
from shared.clients.kinesis_client import KinesisClient
from shared.clients.rds_timescale_client import RDSTimescaleClient
from shared.clients.polygon_client import PolygonClient
# Removed OHLCVData import - using direct dict for performance

# Configure logging
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set specific loggers to INFO to reduce noise
logging.getLogger('aiohttp').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('websockets').setLevel(logging.INFO)

class PolygonWebSocketService:
    def __init__(self):
        # Get Massive API key - support both Secrets Manager (production) and direct env var (local testing)
        # Note: Environment variable name still uses POLYGON_* for backward compatibility
        polygon_secret_arn = os.environ.get('POLYGON_API_KEY_SECRET_ARN')
        if polygon_secret_arn:
            # Production: Use Secrets Manager
            secrets_client = boto3.client('secretsmanager')
            polygon_secret = secrets_client.get_secret_value(SecretId=polygon_secret_arn)
            self.polygon_api_key = json.loads(polygon_secret['SecretString'])['POLYGON_API_KEY']
        else:
            # Local testing: Use direct environment variable
            self.polygon_api_key = os.environ.get('POLYGON_API_KEY')
            if not self.polygon_api_key:
                raise ValueError("Either POLYGON_API_KEY_SECRET_ARN or POLYGON_API_KEY must be set")
        
        # Environment variables
        self.kinesis_stream_name = os.environ.get('KINESIS_STREAM_NAME', 'market-data-raw')
        self.aws_region = os.environ.get('AWS_REGION', 'ca-west-1')
        self.skip_market_check = os.environ.get('SKIP_MARKET_CHECK', 'false').lower() == 'true'
        
        # Initialize clients
        self.polygon_client = PolygonClient(api_key=self.polygon_api_key)
        
        # Initialize RDS client - support both Secrets Manager (production) and direct credentials (local)
        rds_secret_arn = os.environ.get('RDS_SECRET_ARN')
        if rds_secret_arn:
            # Production: Use Secrets Manager
            self.rds_client = RDSTimescaleClient(secret_arn=rds_secret_arn)
        else:
            # Local testing: Use direct credentials from environment variables
            self.rds_client = RDSTimescaleClient(
                endpoint=os.environ.get('POSTGRES_HOST', 'localhost'),
                port=os.environ.get('POSTGRES_PORT', '5432'),
                username=os.environ.get('POSTGRES_USER'),
                password=os.environ.get('POSTGRES_PASSWORD'),
                database=os.environ.get('POSTGRES_DB')
            )
        
        # Initialize Kinesis client
        # Note: boto3 will automatically use AWS_ENDPOINT_URL if set (for LocalStack)
        self.kinesis_client = KinesisClient(
            stream_name=self.kinesis_stream_name,
            region_name=self.aws_region
        )
        
        # Log LocalStack usage if configured
        if os.environ.get('AWS_ENDPOINT_URL'):
            logger.info(f"Using LocalStack endpoint: {os.environ.get('AWS_ENDPOINT_URL')}")
        
        # WebSocket client and state
        self.websocket_client = None
        self.active_symbols = []
        self.running = False
        self.message_count = 0
        self.last_message_time = None
        self.market_check_interval = 300  # Check market status every 5 minutes
        self.last_market_check = None
        
        # Connection health and reconnection
        self.connection_health_check_interval = 60  # Check connection health every 60 seconds
        self.max_idle_time = 300  # Consider connection dead if no messages for 5 minutes
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5  # Start with 5 seconds, exponential backoff
        self.connection_start_time = None
        # Subscription limits (optional cap for local testing)
        self.max_symbols = int(os.environ.get('MAX_SYMBOLS', '0'))  # 0 = no cap
        self.use_wildcard_subscription = os.environ.get('USE_WILDCARD_SUBSCRIPTION', 'false').lower() == 'true'
        
        logger.info("Massive WebSocket Service initialized")
    
    async def check_market_status(self) -> bool:
        """
        Check if market is open (consistent with batch layer pattern)
        
        Returns:
            True if market is open, False if closed
        """
        try:
            if self.skip_market_check:
                logger.debug("⚠️  TESTING MODE: Skipping market status check")
                return True
            
            market_status = self.polygon_client.get_market_status()
            is_open = market_status.get('market') == 'open'
            
            if is_open:
                logger.debug("✅ Market is open")
            else:
                logger.info("⏸️  Market is closed")
            
            self.last_market_check = datetime.utcnow()
            return is_open
            
        except Exception as e:
            logger.error(f"Error checking market status: {str(e)}")
            # On error, assume market is open to avoid blocking
            return True
    
    async def start(self):
        """Start the WebSocket service"""
        try:
            logger.info("Starting Massive WebSocket Service...")
            
            # 1. Check market status (consistent with batch layer pattern)
            # Skip market check if SKIP_MARKET_CHECK=true (for testing)
            if not self.skip_market_check and not await self.check_market_status():
                logger.info("Market is closed - service will wait for market to open")
                # Start a background task to periodically check market status
                asyncio.create_task(self.market_hours_monitor())
                return
            
            # 2. Load active symbols from RDS
            await self.load_active_symbols()
            
            # 3. Initialize Massive WebSocket client  
            await self.initialize_websocket()
            
            # 4. Start market hours monitoring (to pause/resume based on market status)
            asyncio.create_task(self.market_hours_monitor())
            
            # 5. Start connection health monitoring
            asyncio.create_task(self.connection_health_monitor())
            
            # 6. Start the service loop
            await self.run_service_loop()
            
        except Exception as e:
            logger.error(f"Error starting service: {str(e)}")
            raise
    
    async def market_hours_monitor(self):
        """
        Background task to monitor market hours and pause/resume WebSocket connection
        Runs every 5 minutes to check market status
        """
        # Keep monitoring indefinitely (service should run 24/7, but pause when market closed)
        while True:
            try:
                await asyncio.sleep(self.market_check_interval)  # Wait 5 minutes
                
                is_market_open = await self.check_market_status()
                
                if is_market_open:
                    # Market is open - ensure WebSocket is connected
                    if not self.websocket_client or not self.running:
                        logger.info("Market opened - connecting WebSocket...")
                        if not self.active_symbols:
                            await self.load_active_symbols()
                        await self.initialize_websocket()
                        self.running = True
                        # Start service loop in background
                        asyncio.create_task(self.run_service_loop())
                else:
                    # Market is closed - pause WebSocket connection
                    if self.websocket_client and self.running:
                        logger.info("Market closed - pausing WebSocket connection...")
                        try:
                            await self.websocket_client.close()
                        except Exception as close_error:
                            logger.debug(f"Error closing WebSocket (expected): {close_error}")
                        self.running = False
                        self.websocket_client = None
                        
            except Exception as e:
                logger.error(f"Error in market hours monitor: {str(e)}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    async def connection_health_monitor(self):
        """
        Background task to monitor WebSocket connection health
        Detects dead connections and triggers reconnection
        """
        while True:
            try:
                await asyncio.sleep(self.connection_health_check_interval)
                
                # Only check if market is open and we should be connected
                if not self.running or not self.websocket_client:
                    continue
                
                # Check if connection is alive (received messages recently)
                if self.last_message_time:
                    idle_time = (datetime.utcnow() - self.last_message_time).total_seconds()
                    
                    if idle_time > self.max_idle_time:
                        logger.warning(f"⚠️ Connection appears dead - no messages for {idle_time:.0f} seconds")
                        logger.info("Triggering reconnection...")
                        
                        # Close current connection
                        try:
                            logger.info("Closing WebSocket connection...")
                            await self.websocket_client.close()
                            logger.info("WebSocket connection closed")
                        except Exception as close_error:
                            logger.warning(f"Error closing WebSocket during health check: {close_error}")
                        
                        self.running = False
                        self.websocket_client = None
                        
                        # Reinitialize and reconnect
                        try:
                            logger.info("Reinitializing WebSocket...")
                            await self.initialize_websocket()
                            self.running = True
                            logger.info("Starting new service loop...")
                            asyncio.create_task(self.run_service_loop())
                            logger.info("✅ Connection reestablished after health check")
                        except Exception as e:
                            logger.error(f"Error reconnecting after health check: {str(e)}")
                            logger.exception("Full traceback for reconnection error:")
                    elif idle_time > self.max_idle_time / 2:
                        # Warning threshold (half of max idle time)
                        logger.warning(f"⚠️ Connection idle for {idle_time:.0f} seconds (warning threshold: {self.max_idle_time / 2:.0f}s)")
                else:
                    # No messages received yet, but connection exists
                    if self.connection_start_time:
                        connection_age = (datetime.utcnow() - self.connection_start_time).total_seconds()
                        if connection_age > 300:  # 5 minutes with no messages
                            logger.warning(f"⚠️ Connection established {connection_age:.0f} seconds ago but no messages received (count: {self.message_count})")
                
            except Exception as e:
                logger.error(f"Error in connection health monitor: {str(e)}")
                logger.exception("Full traceback for health monitor error:")
                await asyncio.sleep(30)  # Wait 30 seconds before retrying
    
    async def load_active_symbols(self):
        """Load active symbols from RDS symbol_metadata table"""
        try:
            # Use RDS client's get_active_symbols method (synchronous)
            # Since RDS client is synchronous, we'll run it in executor
            import asyncio
            loop = asyncio.get_event_loop()
            symbols = await loop.run_in_executor(
                None,
                self.rds_client.get_active_symbols
            )
            
            if symbols and len(symbols) > 0:
                self.active_symbols = symbols
                logger.info(f"Loaded {len(self.active_symbols)} active symbols from RDS")
            else:
                # Fallback symbols
                self.active_symbols = [
                    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
                    'META', 'NVDA', 'NFLX', 'AMD', 'PYPL'
                ]
                logger.warning(f"Using fallback symbols: {len(self.active_symbols)} symbols")
                
        except Exception as e:
            logger.error(f"Error loading symbols: {str(e)}")
            self.active_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    
    async def initialize_websocket(self):
        """Initialize Massive WebSocket client with active symbols"""
        try:
            # Use wildcard subscription (single message) if enabled
            if self.use_wildcard_subscription:
                subscriptions = ["AM.*"]
                self.websocket_client = WebSocketClient(
                    api_key=self.polygon_api_key,
                    subscriptions=subscriptions,
                    feed=Feed.Delayed  # Use delayed feed: delayed.massive.com (15-min delayed data)
                )
                
                logger.info("Using wildcard subscription: AM.*")
                logger.info("Using delayed feed (15-minute delayed data) - Feed.Delayed")
                logger.info("Massive WebSocket client initialized successfully")
                logger.info(f"WebSocket client type: {type(self.websocket_client)}")
                logger.info("Client subscriptions count: 1 (wildcard)")
                return
            
            # Create subscription list for 1 minute tick (T.*)
            symbols = self.active_symbols
            if self.max_symbols > 0:
                symbols = symbols[:self.max_symbols]
                logger.info(f"Limiting symbols to MAX_SYMBOLS={self.max_symbols}")
            
            subscriptions = [f"AM.{symbol}" for symbol in symbols]
            
            logger.info(f"Initializing WebSocket with {len(subscriptions)} AM.* subscriptions")
            logger.info(f"Sample subscriptions (first 5): {subscriptions[:5]}")
            logger.info(f"Total active symbols: {len(self.active_symbols)}")
            
            # Initialize Massive WebSocket client with delayed feed
            # Using Feed.Delayed for 15-minute delayed data (required when API key doesn't have real-time access)
            self.websocket_client = WebSocketClient(
                api_key=self.polygon_api_key,
                subscriptions=subscriptions,
                feed=Feed.Delayed  # Use delayed feed: delayed.massive.com (15-min delayed data)
            )
            
            logger.info("Using delayed feed (15-minute delayed data) - Feed.Delayed")
            
            logger.info("Massive WebSocket client initialized successfully")
            logger.info(f"WebSocket client type: {type(self.websocket_client)}")
            logger.info(f"Client subscriptions count: {len(subscriptions)}")
            
        except Exception as e:
            logger.error(f"Error initializing WebSocket: {str(e)}")
            logger.exception("Full traceback for WebSocket initialization:")
            raise
    
    async def run_service_loop(self):
        """Main service loop - runs during market hours with automatic reconnection"""
        while True:  # Keep trying to maintain connection
            try:
                if not self.websocket_client:
                    logger.warning("WebSocket client not initialized - skipping service loop")
                    await asyncio.sleep(10)
                    continue
                
                self.running = True
                self.connection_start_time = datetime.utcnow()
                self.reconnect_attempts = 0  # Reset on successful connection
                
                logger.info("Starting WebSocket connection...")
                logger.info(f"Connection start time: {self.connection_start_time}")
                logger.info(f"API key present: {bool(self.polygon_api_key)}")
                logger.info(f"API key length: {len(self.polygon_api_key) if self.polygon_api_key else 0}")
                
                logger.info("Connecting to Massive WebSocket...")
                
                await self.websocket_client.connect(self.handle_websocket_message)
                
                # If we get here, connection closed normally or error occurred
                logger.warning("WebSocket connection closed, will attempt reconnection...")
                self.running = False
                
            except Exception as e:
                logger.error(f"Error in service loop: {str(e)}")
                logger.exception("Full traceback for service loop error:")
                self.running = False
                
            # Exponential backoff reconnection
            if self.reconnect_attempts < self.max_reconnect_attempts:
                wait_time = min(self.reconnect_delay * (2 ** self.reconnect_attempts), 300)  # Max 5 minutes
                
                logger.info(f"Reconnecting in {wait_time} seconds (attempt {self.reconnect_attempts + 1}/{self.max_reconnect_attempts})...")
                await asyncio.sleep(wait_time)
                
                # Reinitialize WebSocket client
                try:
                    await self.initialize_websocket()
                    self.reconnect_attempts += 1
                except Exception as e:
                    logger.error(f"Error reinitializing WebSocket: {str(e)}")
                    await asyncio.sleep(30)  # Wait before retrying
            else:
                logger.error(f"Max reconnection attempts ({self.max_reconnect_attempts}) reached. Waiting for market hours monitor...")
                # Let market_hours_monitor handle reconnection
                await asyncio.sleep(60)
    
    async def handle_websocket_message(self, messages: List[WebSocketMessage]):
        """Handle incoming WebSocket messages from Massive"""
        try:
            logger.debug(f"Received {len(messages)} message(s) from Massive")
            
            if not messages:
                logger.warning("Received empty message list from Massive")
                return
            
            for i, message in enumerate(messages):
                await self.process_aggregate_message(message)
                self.message_count += 1
                self.last_message_time = datetime.utcnow()
                self.reconnect_attempts = 0  # Reset on successful message
                
                # Log first message and then every 100 messages
                if self.message_count == 1:
                    logger.info(f"✅ First message received! Total processed: {self.message_count}")
                elif self.message_count % 100 == 0:
                    logger.info(f"Processed {self.message_count} messages")
                
        except Exception as e:
            logger.error(f"Error handling WebSocket messages: {str(e)}")
            logger.exception("Full traceback for message handling error:")
            # Don't raise - let run_service_loop handle reconnection
    
    async def process_aggregate_message(self, message: WebSocketMessage):
        """
        Process a single aggregate message (AM.* subscription)
        
        OPTIMIZED FOR FAST INGESTION:
        - No datetime conversions (keep as integer milliseconds)
        - No Decimal conversions (keep as float)
        - No intermediate objects (create dict directly)
        - Minimal transformations for maximum throughput
        
        Massive AM.* format:
        {
            "ev": "AM",           # Event type (Aggregate Minute)
            "sym": "AAPL",        # Symbol
            "v": 12345,           # Volume
            "o": 150.85,          # Open price
            "c": 152.90,          # Close price
            "h": 153.17,          # High price
            "l": 150.50,          # Low price
            "a": 151.87,          # VWAP (average)
            "s": 1611082800000,   # Start timestamp (milliseconds)
            "e": 1611082860000    # End timestamp (milliseconds)
        }
        """
        try:
            # Convert WebSocketMessage to dict if needed
            if hasattr(message, '__dict__'):
                data = message.__dict__
            elif hasattr(message, 'data'):
                data = message.data
            else:
                data = message
            
            # Extract symbol - handle both formats:
            # 1. Raw Polygon format: 'sym' (e.g., {'sym': 'AAPL', 'o': 150.85, ...})
            # 2. Transformed format: 'symbol' (e.g., {'symbol': 'ACIW', 'open': 45.61, ...})
            symbol = data.get('sym') or data.get('symbol')
            
            if not symbol:
                logger.warning(f"No symbol found in message: {data}")
                logger.warning(f"Message keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
                return
            
            # Handle both message formats:
            # Raw Polygon format: 'o', 'h', 'l', 'c', 'v', 'e'
            # Transformed format: 'open', 'high', 'low', 'close', 'volume', 'end_timestamp'
            open_price = data.get('o') or data.get('open', 0)
            high_price = data.get('h') or data.get('high', 0)
            low_price = data.get('l') or data.get('low', 0)
            close_price = data.get('c') or data.get('close', 0)
            volume = data.get('v') or data.get('volume', 0)
            end_timestamp_ms = data.get('e') or data.get('end_timestamp') or int(datetime.utcnow().timestamp() * 1000)
            
            # FAST PATH: Create Kinesis record directly with minimal transformations
            # Keep timestamp as integer milliseconds (no datetime conversion)
            # Keep prices as floats (no Decimal conversion)
            # No intermediate OHLCVData object creation
            kinesis_record = {
                'record_type': 'ohlcv',
                'symbol': symbol,
                'open_price': float(open_price),
                'high_price': float(high_price),
                'low_price': float(low_price),
                'close_price': float(close_price),
                'volume': int(volume),
                'timestamp_str': str(end_timestamp_ms),
                'interval_type': '1m',                       # AM.* provides 1-minute aggregates
                'source': 'massive_websocket_am',
                'ingestion_time': int(datetime.utcnow().timestamp() * 1000)  # Milliseconds
            }
            
            # Send directly to Kinesis (no intermediate object creation)
            await self.kinesis_client.put_record(
                data=kinesis_record,
                partition_key=symbol
            )
            
            # Log sample data (first message and then every 100 messages)
            if self.message_count == 1:
                logger.info(f"✅ First Kinesis record sent: {symbol} - O=${kinesis_record['open_price']:.2f} H=${kinesis_record['high_price']:.2f} L=${kinesis_record['low_price']:.2f} C=${kinesis_record['close_price']:.2f} V={kinesis_record['volume']}")
            elif self.message_count % 100 == 0:
                logger.debug(f"Processed {symbol}: O=${kinesis_record['open_price']:.2f} H=${kinesis_record['high_price']:.2f} L=${kinesis_record['low_price']:.2f} C=${kinesis_record['close_price']:.2f} V={kinesis_record['volume']}")
            
        except Exception as e:
            logger.error(f"Error processing aggregate message: {str(e)}")
            logger.error(f"Message data: {data}")
            logger.exception("Full traceback for message processing error:")
    
    async def stop(self):
        """Stop the service gracefully"""
        logger.info("Stopping service...")
        self.running = False
        
        # Close WebSocket connection
        if self.websocket_client:
            await self.websocket_client.close()
        
        # Close shared clients
        if hasattr(self.rds_client, 'close'):
            self.rds_client.close()
        
        if hasattr(self.kinesis_client, 'close'):
            await self.kinesis_client.close()
        
        logger.info("All clients closed successfully")

# Health check server for ECS
class HealthCheckServer:
    def __init__(self, websocket_service, port=8080):
        self.websocket_service = websocket_service
        self.port = port
    
    async def health_check(self, request):
        """ECS health check endpoint"""
        # Service is considered healthy if:
        # 1. WebSocket is connected and running, OR
        # 2. Service is initialized (even if market is closed - that's expected behavior)
        if self.websocket_service.running and self.websocket_service.websocket_client:
            return web.json_response({
                'status': 'healthy',
                'message_count': self.websocket_service.message_count,
                'last_message': self.websocket_service.last_message_time.isoformat() if self.websocket_service.last_message_time else None
            })
        elif self.websocket_service.polygon_client:  # Service is initialized, market might be closed
            # Check market status to provide context
            is_market_open = await self.websocket_service.check_market_status()
            return web.json_response({
                'status': 'healthy',
                'message': 'Market is closed, waiting for open' if not is_market_open else 'Service initialized, connecting...',
                'market_open': is_market_open
            })
        else:
            return web.json_response({'status': 'unhealthy'}, status=503)
    
    async def start(self):
        """Start health check server"""
        app = web.Application()
        app.router.add_get('/health', self.health_check)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        
        logger.info(f"Health check server started on port {self.port}")

async def main():
    """Main entry point for ECS service"""
    websocket_service = PolygonWebSocketService()
    health_server = HealthCheckServer(websocket_service)
    
    # Handle shutdown signals
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(websocket_service.stop())
        raise GracefulExit()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Start both services
        await asyncio.gather(
            health_server.start(),
            websocket_service.start()
        )
    except (GracefulExit, KeyboardInterrupt):
        await websocket_service.stop()
    except Exception as e:
        logger.error(f"Service error: {str(e)}")
        await websocket_service.stop()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())