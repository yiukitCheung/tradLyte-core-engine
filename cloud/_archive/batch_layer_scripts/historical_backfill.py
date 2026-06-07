"""
Historical OHLCV Backfill using AWS Batch
Purpose: Fetch 5 years of historical OHLCV data for new symbols

This is designed for AWS Batch (no timeout limits) to backfill 
thousands of symbols with full historical data.

Architecture:
- Fetch from Polygon API (async, 5 concurrent)
- Write to S3 Bronze layer (Parquet files)
- Write to RDS (5-year retention cache)
- Update watermark table
"""

import os
import sys
import logging
import time
import asyncio
import aiohttp
import boto3
import json
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any
from decimal import Decimal
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA MODEL
# ============================================================================

@dataclass
class OHLCVData:
    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    interval: str = "1d"


# ============================================================================
# POLYGON CLIENT (Async for high performance)
# ============================================================================

class PolygonHistoricalClient:
    """Async Polygon client optimized for historical data fetching"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
    
    async def fetch_historical_ohlcv(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        from_date: date,
        to_date: date
    ) -> List[OHLCVData]:
        """Fetch 5 years of daily OHLCV in ONE API call"""
        try:
            from_str = from_date.strftime('%Y-%m-%d')
            to_str = to_date.strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/day/{from_str}/{to_str}"
            params = {
                'apiKey': self.api_key,
                'adjusted': 'true',
                'sort': 'asc',
                'limit': 50000
            }
            
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    logger.warning(f"API failed for {symbol}: {response.status}")
                    return []
                
                data = await response.json()
                results = data.get('results', [])
                
                if not results:
                    return []
                
                ohlcv_list = []
                for bar in results:
                    try:
                        ohlcv = OHLCVData(
                            symbol=symbol,
                            timestamp=datetime.fromtimestamp(bar['t'] / 1000),
                            open=Decimal(str(bar['o'])),
                            high=Decimal(str(bar['h'])),
                            low=Decimal(str(bar['l'])),
                            close=Decimal(str(bar['c'])),
                            volume=int(bar['v']),
                            interval="1d"
                        )
                        ohlcv_list.append(ohlcv)
                    except Exception as e:
                        continue
                
                return ohlcv_list
                
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return []
    
    async def fetch_batch_historical(
        self,
        symbols: List[str],
        from_date: date,
        to_date: date,
        max_concurrent: int = 5
    ) -> Dict[str, List[OHLCVData]]:
        """Fetch historical data for multiple symbols concurrently"""
        connector = aiohttp.TCPConnector(limit=max_concurrent)
        timeout = aiohttp.ClientTimeout(total=120)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [
                self.fetch_historical_ohlcv(session, symbol, from_date, to_date)
                for symbol in symbols
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            symbol_data = {}
            for symbol, result in zip(symbols, results):
                if isinstance(result, Exception):
                    logger.error(f"Exception for {symbol}: {result}")
                    symbol_data[symbol] = []
                else:
                    symbol_data[symbol] = result
            
            return symbol_data


# ============================================================================
# HISTORICAL BACKFILL JOB
# ============================================================================

class HistoricalBackfillJob:
    """AWS Batch job for historical OHLCV backfill"""
    
    def __init__(
        self,
        polygon_api_key: str,
        s3_bucket: str,
        rds_host: str,
        rds_database: str,
        rds_user: str,
        rds_password: str,
        years_back: int = 5
    ):
        self.polygon_client = PolygonHistoricalClient(polygon_api_key)
        self.s3_bucket = s3_bucket
        self.s3_bronze_prefix = "bronze/raw_ohlcv"
        self.s3_client = boto3.client('s3')
        self.years_back = years_back
        
        # RDS connection
        self.rds_conn = psycopg2.connect(
            host=rds_host,
            database=rds_database,
            user=rds_user,
            password=rds_password,
            port=5432
        )
        self.rds_conn.autocommit = True
        
        logger.info(f"Historical Backfill Job initialized")
        logger.info(f"S3 Bucket: {s3_bucket}")
        logger.info(f"Years back: {years_back}")
    
    def get_symbols_needing_backfill(self) -> List[str]:
        """Find symbols with limited history"""
        query = """
            WITH symbol_record_counts AS (
                SELECT symbol, COUNT(*) as record_count
                FROM raw_ohlcv
                WHERE interval = '1d'
                GROUP BY symbol
            )
            SELECT sm.symbol
            FROM symbol_metadata sm
            LEFT JOIN symbol_record_counts src ON sm.symbol = src.symbol
            WHERE LOWER(sm.active) = 'true'
            AND COALESCE(src.record_count, 0) < 100  -- Less than ~6 months of data
            ORDER BY src.record_count ASC NULLS FIRST;
        """
        
        with self.rds_conn.cursor() as cursor:
            cursor.execute(query)
            symbols = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Found {len(symbols)} symbols needing backfill")
        return symbols
    
    def write_to_s3(self, ohlcv_list: List[OHLCVData], symbol: str) -> int:
        """Write OHLCV data to S3 bronze layer"""
        if not ohlcv_list:
            return 0
        
        # Group by date
        date_groups = {}
        for ohlcv in ohlcv_list:
            ohlcv_date = ohlcv.timestamp.date()
            if ohlcv_date not in date_groups:
                date_groups[ohlcv_date] = []
            date_groups[ohlcv_date].append(ohlcv)
        
        records_written = 0
        for ohlcv_date, day_data in date_groups.items():
            s3_key = f"{self.s3_bronze_prefix}/symbol={symbol}/date={ohlcv_date.isoformat()}.parquet"
            
            table = pa.table({
                'symbol': [ohlcv.symbol for ohlcv in day_data],
                'open': [float(ohlcv.open) for ohlcv in day_data],
                'high': [float(ohlcv.high) for ohlcv in day_data],
                'low': [float(ohlcv.low) for ohlcv in day_data],
                'close': [float(ohlcv.close) for ohlcv in day_data],
                'volume': [int(ohlcv.volume) for ohlcv in day_data],
                'timestamp': [ohlcv.timestamp for ohlcv in day_data],
                'interval': [ohlcv.interval for ohlcv in day_data]
            })
            
            parquet_buffer = BytesIO()
            pq.write_table(table, parquet_buffer, compression='snappy')
            parquet_buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/x-parquet'
            )
            
            records_written += len(day_data)
        
        return records_written
    
    def write_to_rds(self, ohlcv_list: List[OHLCVData]) -> int:
        """Write OHLCV data to RDS with 5-year retention"""
        if not ohlcv_list:
            return 0
        
        # Filter to last 5 years
        retention_threshold = date.today() - timedelta(days=365 * 5 + 30)
        filtered = [o for o in ohlcv_list if o.timestamp.date() >= retention_threshold]
        
        if not filtered:
            return 0
        
        sql = """
            INSERT INTO raw_ohlcv (symbol, open, high, low, close, volume, timestamp, interval)
            VALUES %s
            ON CONFLICT (symbol, timestamp, interval) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """
        
        data_tuples = [
            (o.symbol, float(o.open), float(o.high), float(o.low), 
             float(o.close), o.volume, o.timestamp, o.interval)
            for o in filtered
        ]
        
        with self.rds_conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, sql, data_tuples, page_size=1000)
        
        return len(filtered)
    
    def update_watermark(self, symbol: str, latest_date: date):
        """Update watermark table"""
        with self.rds_conn.cursor() as cursor:
            # Mark old as not current
            cursor.execute("""
                UPDATE data_ingestion_watermark
                SET is_current = FALSE
                WHERE symbol = %s AND is_current = TRUE
            """, (symbol,))
            
            # Insert new current
            cursor.execute("""
                INSERT INTO data_ingestion_watermark 
                    (symbol, latest_date, ingested_at, records_count, is_current)
                VALUES (%s, %s, NOW(), 1, TRUE)
            """, (symbol, latest_date))
    
    def run(self, symbols: List[str] = None, batch_size: int = 50) -> Dict[str, Any]:
        """Run the historical backfill"""
        start_time = time.time()
        
        # Get symbols to backfill
        if not symbols:
            symbols = self.get_symbols_needing_backfill()
        
        if not symbols:
            logger.info("No symbols need backfill")
            return {'symbols_processed': 0, 'total_records': 0}
        
        # Calculate date range
        to_date = date.today()
        from_date = to_date - timedelta(days=365 * self.years_back)
        
        logger.info(f"📅 Date range: {from_date} to {to_date}")
        logger.info(f"📊 Processing {len(symbols)} symbols in batches of {batch_size}")
        
        total_records = 0
        symbols_processed = 0
        
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(symbols) + batch_size - 1) // batch_size
            
            logger.info(f"📦 Batch {batch_num}/{total_batches}: {len(batch_symbols)} symbols")
            
            # Fetch historical data
            symbol_data = asyncio.run(
                self.polygon_client.fetch_batch_historical(
                    batch_symbols, from_date, to_date, max_concurrent=5
                )
            )
            
            # Process each symbol
            for symbol, ohlcv_list in symbol_data.items():
                if not ohlcv_list:
                    continue
                
                # Write to S3
                s3_records = self.write_to_s3(ohlcv_list, symbol)
                
                # Write to RDS
                rds_records = self.write_to_rds(ohlcv_list)
                
                # Update watermark
                latest_date = max(o.timestamp.date() for o in ohlcv_list)
                self.update_watermark(symbol, latest_date)
                
                total_records += len(ohlcv_list)
                symbols_processed += 1
            
            logger.info(f"  ✅ Batch complete: {symbols_processed}/{len(symbols)} symbols, {total_records:,} records")
        
        elapsed = time.time() - start_time
        
        logger.info("=" * 60)
        logger.info("✅ HISTORICAL BACKFILL COMPLETE")
        logger.info("=" * 60)
        logger.info(f"📊 Symbols processed: {symbols_processed}")
        logger.info(f"💾 Total records: {total_records:,}")
        logger.info(f"📅 Date range: {from_date} to {to_date}")
        logger.info(f"⏱️  Elapsed time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        
        return {
            'symbols_processed': symbols_processed,
            'total_records': total_records,
            'elapsed_seconds': elapsed
        }


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point for AWS Batch"""
    logger.info("=" * 60)
    logger.info("🚀 HISTORICAL BACKFILL JOB STARTING")
    logger.info("=" * 60)
    
    # Load .env file if running locally
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logger.info("Loaded environment from .env file")
    except ImportError:
        pass  # dotenv not available in AWS Batch container
    
    # Get configuration from environment
    polygon_api_key = os.environ.get('POLYGON_API_KEY')
    s3_bucket = os.environ.get('S3_DATALAKE_BUCKET', 'dev-condvest-datalake')
    rds_host = os.environ.get('RDS_HOST')
    rds_database = os.environ.get('RDS_DATABASE', 'condvest')
    rds_user = os.environ.get('RDS_USER', 'postgres')
    rds_password = os.environ.get('RDS_PASSWORD')
    years_back = int(os.environ.get('YEARS_BACK', '5'))
    
    # Optional: specific symbols from environment (comma-separated)
    symbols_env = os.environ.get('SYMBOLS')
    symbols = symbols_env.split(',') if symbols_env else None
    
    if not polygon_api_key:
        logger.error("❌ POLYGON_API_KEY environment variable required")
        sys.exit(1)
    
    if not rds_host or not rds_password:
        logger.error("❌ RDS connection details required (RDS_HOST, RDS_PASSWORD)")
        sys.exit(1)
    
    try:
        job = HistoricalBackfillJob(
            polygon_api_key=polygon_api_key,
            s3_bucket=s3_bucket,
            rds_host=rds_host,
            rds_database=rds_database,
            rds_user=rds_user,
            rds_password=rds_password,
            years_back=years_back
        )
        
        result = job.run(symbols=symbols)
        
        logger.info(f"✅ Job completed successfully: {result}")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Job failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()

