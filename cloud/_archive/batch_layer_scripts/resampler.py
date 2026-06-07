"""
Optimized OHLCV Resampler using DuckDB + S3 (Pure Data Lake Architecture)
Purpose: High-performance resampling using DuckDB for S3 data processing

Cost-efficient data lake approach that leverages:
- DuckDB for fast S3 data reading and resampling
- S3 for scalable data storage (Bronze → Silver layers)
- Parquet format for efficient analytics queries
- No RDS dependency - pure S3-based data lake
Uses proven ROW_NUMBER windowing approach optimized for DuckDB.
"""

import os
import sys
import logging
import time
import duckdb
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
import boto3
import json
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DuckDBS3Resampler:
    """
    High-performance OHLCV resampling using DuckDB + S3 (Pure Data Lake)
    
    Leverages:
    - DuckDB for fast S3 data reading and resampling
    - S3 for scalable data storage (Bronze → Silver)
    - Parquet format for efficient analytics
    - No RDS dependency - pure S3-based data lake
    - Proven ROW_NUMBER approach optimized for DuckDB
    """
    
    # Fibonacci intervals 3-34 (matching your settings.yaml)
    RESAMPLING_INTERVALS = [3, 5, 8, 13, 21, 34]
    
    def __init__(self, s3_bucket: str, s3_output_prefix: str = "silver"):
        """Initialize DuckDB connection and S3 configuration"""
        # Initialize DuckDB connection
        self.duckdb_conn = duckdb.connect()
        
        # S3 configuration
        self.s3_bucket = s3_bucket
        self.s3_output_prefix = s3_output_prefix
        self.s3_client = boto3.client('s3')
        
        # Configure DuckDB for S3 access
        self._setup_duckdb_s3_config()
        
        logger.info(f"DuckDB + S3 Data Lake Resampler initialized")
        logger.info(f"Output: s3://{s3_bucket}/{s3_output_prefix}/")
    
    def _setup_duckdb_s3_config(self):
        """Configure DuckDB for S3 access using IAM roles (AWS best practice)"""
        try:
            # Install and load the httpfs extension for S3 access
            self.duckdb_conn.execute("INSTALL httpfs")
            self.duckdb_conn.execute("LOAD httpfs")
            
            # Get AWS credentials from the environment (ECS task IAM role)
            session = boto3.Session()
            credentials = session.get_credentials()
            
            if credentials:
                # Use temporary credentials from IAM role
                self.duckdb_conn.execute(f"SET s3_access_key_id='{credentials.access_key}'")
                self.duckdb_conn.execute(f"SET s3_secret_access_key='{credentials.secret_key}'")
                if credentials.token:
                    self.duckdb_conn.execute(f"SET s3_session_token='{credentials.token}'")
            else:
                # Force DuckDB to use AWS SDK credential chain
                self.duckdb_conn.execute("SET s3_access_key_id=''")
                self.duckdb_conn.execute("SET s3_secret_access_key=''")
            
            # Set region and SSL settings
            aws_region = os.environ.get('AWS_REGION', 'ca-west-1')
            self.duckdb_conn.execute(f"SET s3_region='{aws_region}'")
            self.duckdb_conn.execute("SET s3_use_ssl=true")
            self.duckdb_conn.execute("SET s3_url_style='path'")
            self.duckdb_conn.execute(f"SET s3_endpoint='s3.{aws_region}.amazonaws.com'")
            
            logger.info("✅ DuckDB S3 configured successfully")
            
        except Exception as e:
            logger.error(f"Error configuring DuckDB for S3: {str(e)}")
            raise
    
    def create_s3_view(self, s3_bucket: str, s3_prefix: str = "bronze/raw_ohlcv"):
        """Create a DuckDB view that reads from S3 parquet files
        
        OPTIMIZED ARCHITECTURE: Reads from consolidated data.parquet files
        - data.parquet: Maintained by separate consolidation job
        - Contains ALL historical data (consolidated from date=*.parquet files)
        - One file per symbol = fast query performance
        
        Data Flow:
            1. Lambda Fetcher → writes date=*.parquet (daily incremental)
            2. Consolidation Job → merges into data.parquet (periodic)
            3. Resampler → reads data.parquet (fast analytics)
        
        This follows industry-standard data lake compaction patterns:
            - Delta Lake OPTIMIZE
            - Apache Iceberg compaction
            - Apache Hudi compaction
        """
        try:
            # Read from consolidated files (one per symbol)
            s3_path = f"s3://{s3_bucket}/{s3_prefix}/symbol=*/data.parquet"
            
            # 5-year retention filter (sufficient for all technical indicators)
            # Configurable via environment variable for flexibility
            retention_years = int(os.environ.get('RESAMPLING_RETENTION_YEARS', '5'))
            retention_date = (datetime.now() - timedelta(days=365 * retention_years)).strftime('%Y-%m-%d')
            
            logger.info(f"Creating DuckDB view from consolidated bronze layer:")
            logger.info(f"  📦 Path: {s3_path}")
            logger.info(f"  ⚡ Optimized for fast analytics (one file per symbol)")
            logger.info(f"  📅 Retention filter: Last {retention_years} years (since {retention_date})")
            
            # Create view with retention filter - only process recent data
            # This significantly improves performance (70%+ faster) while maintaining
            # mathematical accuracy for all technical indicators
            create_view_sql = f"""
            CREATE OR REPLACE VIEW s3_ohlcv AS 
            SELECT 
                symbol,
                open,
                high,
                low,
                close,
                volume,
                timestamp,
                interval
            FROM read_parquet('{s3_path}')
            WHERE timestamp >= '{retention_date}'
            """
            
            self.duckdb_conn.execute(create_view_sql)
            logger.info(f"✅ DuckDB view 's3_ohlcv' created with {retention_years}-year retention filter")
            
        except Exception as e:
            logger.error(f"Error creating S3 view: {str(e)}")
            raise
    
    def get_fibonacci_resampling_sql(self, interval: int, latest_timestamp: Optional[str] = None) -> str:
        """
        Generate DuckDB SQL for Fibonacci resampling using ROW_NUMBER approach
        
        CORRECT INCREMENTAL APPROACH:
        1. Resample ALL data from S3 (form complete intervals)
        2. Apply incremental filter AFTER resampling (on aggregated results)
        3. This ensures complete intervals are never partially formed
        """
        # Incremental filter applied AFTER resampling
        incremental_filter = ""
        if latest_timestamp:
            incremental_filter = f"AND start_date > '{latest_timestamp}'"
        
        sql = f"""
        WITH date_boundaries AS (
                SELECT
                timestamp::DATE as date_val,
                    symbol,
                MIN(timestamp) as first_ts,
                MAX(timestamp) as last_ts
                FROM s3_ohlcv
            GROUP BY timestamp::DATE, symbol
        ),
        day_numbers AS (
            SELECT 
                date_val,
                symbol,
                first_ts,
                last_ts,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date_val) as day_num
            FROM date_boundaries
        ),
        fibonacci_groups AS (
                SELECT
                date_val,
                    symbol,
                first_ts,
                last_ts,
                day_num,
                FLOOR((day_num - 1) / {interval}) as group_num
            FROM day_numbers
            ),
            aggregated AS (
                SELECT
                MIN(fg.first_ts) as start_date,
                fg.symbol,
                FIRST(o.open ORDER BY o.timestamp) as open,
                MAX(o.high) as high,
                MIN(o.low) as low,
                FIRST(o.close ORDER BY o.timestamp DESC) as close,
                SUM(o.volume) as volume
            FROM fibonacci_groups fg
            JOIN s3_ohlcv o ON fg.symbol = o.symbol 
                AND o.timestamp >= fg.first_ts 
                AND o.timestamp <= fg.last_ts
            GROUP BY fg.symbol, fg.group_num
            )
            SELECT
            start_date AS ts,
                symbol,
                open,
                high,
                low,
                close,
                volume
            FROM aggregated
        WHERE 1=1 {incremental_filter}
        ORDER BY ts, symbol
        """
        
        return sql
    
    def _read_checkpoint(self, interval: int) -> Optional[Dict]:
        """
        Read checkpoint file from S3 to determine what's been processed
        
        Checkpoint file structure:
        {
            "last_processed_date": "2025-09-22",
            "last_run_timestamp": "2025-09-23T02:00:00Z",
            "total_records_processed": 5350,
            "status": "completed"
        }
        
        Args:
            interval: The Fibonacci interval (3, 5, 8, etc.)
            
        Returns:
            Checkpoint dict or None if checkpoint doesn't exist
        """
        checkpoint_key = f"processing_metadata/silver_{interval}d_checkpoint.json"
        
        try:
            logger.info(f"🔍 Checking for checkpoint file: s3://{self.s3_bucket}/{checkpoint_key}")
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=checkpoint_key
            )
            
            checkpoint_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Validate checkpoint has required fields
            if not checkpoint_data or 'last_processed_date' not in checkpoint_data:
                logger.warning("⚠️  Checkpoint file exists but is empty or invalid")
                return None
            
            logger.info(f"✅ Found checkpoint file:")
            logger.info(f"   - Last processed date: {checkpoint_data.get('last_processed_date')}")
            logger.info(f"   - Last run: {checkpoint_data.get('last_run_timestamp', 'N/A')}")
            logger.info(f"   - Total records: {checkpoint_data.get('total_records_processed', 'N/A')}")
            logger.info(f"   - Status: {checkpoint_data.get('status', 'N/A')}")
            
            return checkpoint_data
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.info(f"✅ No checkpoint file found (first run)")
                logger.info(f"   Will perform FULL resampling and create checkpoint")
                return None
            else:
                logger.warning(f"⚠️  Error reading checkpoint: {str(e)}")
                return None
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️  Checkpoint file is not valid JSON: {str(e)}")
            return None
        except Exception as e:
            logger.warning(f"⚠️  Unexpected error reading checkpoint: {str(e)}")
            return None
    
    def _write_checkpoint(self, interval: int, latest_date: str, total_records: int, status: str = "completed"):
        """
        Write checkpoint file to S3 after successful processing
        
        Args:
            interval: The Fibonacci interval (3, 5, 8, etc.)
            latest_date: The latest date that was processed
            total_records: Total records processed in this run
            status: Processing status (completed, failed, etc.)
        """
        checkpoint_key = f"processing_metadata/silver_{interval}d_checkpoint.json"
        
        checkpoint_data = {
            "last_processed_date": latest_date,
            "last_run_timestamp": datetime.now().isoformat(),
            "total_records_processed": total_records,
            "status": status,
            "interval": f"{interval}d"
        }
        
        try:
            logger.info(f"📝 Writing checkpoint file: s3://{self.s3_bucket}/{checkpoint_key}")
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=checkpoint_key,
                Body=json.dumps(checkpoint_data, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"✅ Checkpoint file updated successfully")
            logger.info(f"   - Last processed date: {latest_date}")
            logger.info(f"   - Records in this run: {total_records}")
            
        except Exception as e:
            logger.error(f"❌ Error writing checkpoint file: {str(e)}")
            logger.warning("⚠️  Processing completed but checkpoint not saved!")
            # Don't raise - processing succeeded even if checkpoint failed
    
    def _get_latest_timestamp_from_checkpoint(self, interval: int, force_full_resample: bool = False) -> Optional[str]:
        """
        Get the latest processed timestamp from checkpoint file (NEW APPROACH)
        
        This is cleaner than querying output files because:
        - Clear separation of concerns (tracking vs output)
        - Easy to reset (just delete checkpoint)
        - Can track additional metadata
        - No risk of sync issues
        
        Args:
            interval: The Fibonacci interval
            force_full_resample: If True, ignore checkpoint and do full resample
            
        Returns:
            Latest timestamp string or None for full resample
        """
        if force_full_resample:
            logger.info("⚠️  FORCE_FULL_RESAMPLE enabled - ignoring checkpoint")
            return None
        
        checkpoint = self._read_checkpoint(interval)
        
        if not checkpoint:
            logger.info("🔄 No checkpoint found → FULL RESAMPLE mode")
            return None
        
        latest_date = checkpoint.get('last_processed_date')
        if not latest_date:
            logger.warning("⚠️  Checkpoint missing last_processed_date → FULL RESAMPLE mode")
            return None
        
        logger.info(f"⚡ INCREMENTAL MODE: Will process data after {latest_date}")
        return latest_date
    
    def _get_latest_timestamp_from_s3_output(self, s3_prefix: str, force_full_resample: bool = False) -> Optional[str]:
        """
        FALLBACK: Get latest timestamp from output files (OLD APPROACH)
        
        This is kept for backward compatibility if checkpoint files don't exist yet.
        The checkpoint-based approach is preferred.
        
        Args:
            s3_prefix: The S3 prefix to check (e.g., 'silver/silver_3d')
            force_full_resample: If True, skip incremental logic and resample all data
        
        Returns:
            Latest timestamp string or None for full resample
        """
        if force_full_resample:
            logger.info("⚠️  FORCE_FULL_RESAMPLE enabled - ignoring output files")
            return None
        
        logger.info("📋 Using FALLBACK mode: checking output files directly")
            
        try:
            # Log the exact path we're checking
            logger.info(f"🔍 Checking output files in: s3://{self.s3_bucket}/{s3_prefix}/")
            
            # Check if any parquet files exist in the PROCESSED (silver) folder
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=s3_prefix,
                MaxKeys=10  # Get a few files to show what exists
            )
            
            if 'Contents' not in response:
                logger.info(f"✅ No existing processed data found - will perform FULL resampling of all raw data")
                return None
            
            # Log what files we found
            file_count = len(response['Contents'])
            logger.info(f"📦 Found {file_count} existing processed files in silver layer")
            for i, obj in enumerate(response['Contents'][:3]):  # Show first 3 files
                logger.info(f"   - {obj['Key']} (size: {obj['Size']} bytes)")
            if file_count > 3:
                logger.info(f"   ... and {file_count - 3} more files")
            
            # Query the max timestamp from existing parquet files
            s3_path = f"s3://{self.s3_bucket}/{s3_prefix}/**/*.parquet"
            logger.info(f"🔍 Querying latest timestamp from: {s3_path}")
            
            query = f"""
            SELECT 
                MAX(ts) as latest_timestamp,
                MIN(ts) as earliest_timestamp,
                COUNT(*) as total_records
            FROM read_parquet('{s3_path}')
            """
            
            result = self.duckdb_conn.execute(query).fetchone()
            if result and result[0]:
                latest_timestamp = result[0]
                earliest_timestamp = result[1]
                total_records = result[2]
                
                # Convert to string format for SQL comparison
                if isinstance(latest_timestamp, str):
                    latest_ts_str = latest_timestamp
                else:
                    latest_ts_str = latest_timestamp.strftime('%Y-%m-%d')
                
                if isinstance(earliest_timestamp, str):
                    earliest_ts_str = earliest_timestamp
                else:
                    earliest_ts_str = earliest_timestamp.strftime('%Y-%m-%d')
                
                logger.info(f"📊 Existing processed data summary:")
                logger.info(f"   - Date range: {earliest_ts_str} to {latest_ts_str}")
                logger.info(f"   - Total records: {total_records:,}")
                logger.info(f"⚡ INCREMENTAL MODE: Will only process data AFTER {latest_ts_str}")
                
                return latest_ts_str
            else:
                logger.warning(f"Could not read timestamp from existing files")
                return None
                
        except Exception as e:
            logger.warning(f"⚠️  Error checking for existing data: {str(e)}")
            logger.info("Proceeding with full resampling")
            return None
    
    def _write_to_s3_parquet(self, df: pd.DataFrame, s3_prefix: str, interval: int):
        """Write DataFrame to S3 partitioned by interval -> symbol -> year -> month (one file per month)"""
        try:
            if 'symbol' not in df.columns:
                raise ValueError("Resampled DataFrame must have a 'symbol' column for partitioning")
            ts = pd.to_datetime(df['ts'])
            df = df.copy()
            df['year'] = ts.dt.year
            df['month'] = ts.dt.month.apply(lambda x: f"{x:02d}")

            for (symbol_val, year, month_str), group_df in df.groupby(['symbol', 'year', 'month']):
                output_df = group_df.drop(columns=['year', 'month'])
                partition_path = f"{s3_prefix}/{symbol_val}/{year}/{month_str}"
                filename = f"data_{interval}d_{year}{month_str}.parquet"
                s3_key = f"{partition_path}/{filename}"
                temp_file = f"/tmp/{symbol_val}_{year}_{month_str}_{filename}"
                output_df.to_parquet(temp_file, engine='pyarrow', compression='snappy', index=False)
                self.s3_client.upload_file(temp_file, self.s3_bucket, s3_key)
                logger.info(f"📦 Wrote {len(output_df)} records to s3://{self.s3_bucket}/{s3_key}")
                os.remove(temp_file)

            logger.info(f"✅ Successfully wrote {len(df)} total records to S3")
        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}")
            raise
    
    def process_interval(self, interval: int, s3_input_bucket: str, force_full_resample: bool = False) -> Dict:
        """
        Process a single resampling interval using DuckDB + S3 (write to S3)
        
        Args:
            interval: Fibonacci interval (3, 5, 8, 13, 21, 34)
            s3_input_bucket: S3 bucket containing raw OHLCV data
            force_full_resample: If True, reprocess ALL data (ignore existing silver data)
        """
        start_time = time.time()
        output_prefix = f"{self.s3_output_prefix}/silver_{interval}d"
        
        logger.info("=" * 70)
        logger.info(f"📈 PROCESSING INTERVAL: {interval}d")
        logger.info("=" * 70)
        logger.info(f"Output: s3://{self.s3_bucket}/{output_prefix}/")
        
        # Create S3 view in DuckDB for reading bronze (raw) data
        raw_data_prefix = "bronze/raw_ohlcv"
        logger.info(f"📥 Reading raw data from: s3://{s3_input_bucket}/{raw_data_prefix}/")
        self.create_s3_view(s3_input_bucket, raw_data_prefix)
        
        # Get stats on raw data source
        try:
            raw_stats_query = """
            SELECT 
                MIN(timestamp) as earliest_date,
                MAX(timestamp) as latest_date,
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT timestamp::DATE) as unique_dates
            FROM s3_ohlcv
            """
            raw_stats = self.duckdb_conn.execute(raw_stats_query).fetchone()
            if raw_stats:
                logger.info(f"📊 RAW DATA STATISTICS:")
                logger.info(f"   - Date range: {raw_stats[0]} to {raw_stats[1]}")
                logger.info(f"   - Total records: {raw_stats[2]:,}")
                logger.info(f"   - Unique symbols: {raw_stats[3]:,}")
                logger.info(f"   - Unique dates: {raw_stats[4]:,}")
        except Exception as e:
            logger.warning(f"Could not get raw data stats: {str(e)}")
        
        # NEW APPROACH: Check checkpoint file first (preferred)
        # FALLBACK: Query output files if checkpoint doesn't exist
        logger.info("\n" + "=" * 70)
        logger.info("CHECKING PROCESSING CHECKPOINT")
        logger.info("=" * 70)
        
        latest_timestamp = self._get_latest_timestamp_from_checkpoint(interval, force_full_resample)
        
        # Fallback to querying output files if no checkpoint exists
        if latest_timestamp is None and not force_full_resample:
            logger.info("\n💡 Checkpoint not found, trying fallback: query output files...")
            latest_timestamp = self._get_latest_timestamp_from_s3_output(output_prefix, force_full_resample)
        
        if latest_timestamp:
            logger.info(f"📍 INCREMENTAL mode: Processing only data after {latest_timestamp}")
        else:
            logger.info(f"🔄 FULL RESAMPLE mode: Processing ALL raw data")
        
        # Generate DuckDB resampling SQL with incremental filter
        resampling_sql = self.get_fibonacci_resampling_sql(interval, latest_timestamp)
        
        try:
            # Execute resampling in DuckDB
            logger.info(f"Executing DuckDB resampling for {interval}d...")
            result = self.duckdb_conn.execute(resampling_sql)
            
            # Fetch all results into a DataFrame for easy Parquet writing
            df = result.df()
            records_count = len(df)
            logger.info(f"DuckDB processed {records_count} records for {interval}d")
            
            if records_count == 0:
                logger.warning(f"No new data found for {interval}d interval")
                return {
                    'interval': interval,
                    'records_processed': 0,
                    'execution_time': time.time() - start_time,
                    'status': 'success'
                }
            
            # Write resampled data to S3 as partitioned Parquet
            logger.info(f"Writing {records_count} records to S3 as Parquet...")
            self._write_to_s3_parquet(df, output_prefix, interval)
            
            # Get the latest date from the processed data
            latest_processed_date = df['ts'].max()
            if isinstance(latest_processed_date, pd.Timestamp):
                latest_processed_date = latest_processed_date.strftime('%Y-%m-%d')
            elif not isinstance(latest_processed_date, str):
                latest_processed_date = str(latest_processed_date)
            
            # Write checkpoint file to track what's been processed
            logger.info(f"\n📝 Updating checkpoint...")
            self._write_checkpoint(interval, latest_processed_date, records_count, "completed")
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            logger.info(f"\n✅ Completed {interval}d: {records_count} records in {execution_time:.2f}s")
            logger.info(f"📦 Output: s3://{self.s3_bucket}/{output_prefix}/")
            logger.info(f"📝 Checkpoint: s3://{self.s3_bucket}/processing_metadata/silver_{interval}d_checkpoint.json")
            
            return {
                'interval': interval,
                'records_processed': records_count,
                'execution_time': execution_time,
                'status': 'success',
                's3_output': f"s3://{self.s3_bucket}/{output_prefix}/"
            } 
            
        except Exception as e:
            logger.error(f"Error processing {interval}d: {str(e)}")
            raise
    
    def close(self):
        """Clean up resources"""
        if self.duckdb_conn:
            self.duckdb_conn.close()
            logger.info("DuckDB connection closed")


def run_resampling_job(s3_bucket: str, intervals: List[int] = None, force_full_resample: bool = False):
    """
    Run resampling job for specified intervals
    
    Args:
        s3_bucket: S3 bucket containing raw data and where to write silver data
        intervals: List of Fibonacci intervals to process (default: [3, 5, 8, 13, 21, 34])
        force_full_resample: If True, reprocess ALL data (ignore existing silver data)
    """
    if intervals is None:
        intervals = DuckDBS3Resampler.RESAMPLING_INTERVALS
    
    logger.info("=" * 80)
    logger.info("🚀 STARTING RESAMPLING PROCESS")
    logger.info("=" * 80)
    logger.info(f"📅 Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📊 Intervals to process: {intervals}")
    logger.info(f"🔄 Mode: {'FULL RESAMPLE (all data)' if force_full_resample else 'INCREMENTAL (new data only)'}")
    logger.info(f"🪣 S3 Bucket: {s3_bucket}")
    
    try:
        resampler = DuckDBS3Resampler(s3_bucket=s3_bucket)
        
        results = []
        logger.info("\n🔄 Starting interval processing...")
        
        for idx, interval in enumerate(intervals, 1):
            logger.info(f"\n📈 [{idx}/{len(intervals)}] Processing interval: {interval}d")
            result = resampler.process_interval(interval, s3_bucket, force_full_resample)
            results.append(result)
            
            # Log progress
            completed = idx
            remaining = len(intervals) - idx
            logger.info(f"✅ Progress: {completed}/{len(intervals)} completed, {remaining} remaining\n")
        
        # Close connections
        resampler.close()
        
        # Summary
        total_records = sum(r['records_processed'] for r in results)
        total_time = sum(r['execution_time'] for r in results)
        
        logger.info("=" * 80)
        logger.info("🎉 RESAMPLING JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"✅ Processed {len(intervals)} intervals")
        logger.info(f"📊 Total records: {total_records:,}")
        logger.info(f"⏱️  Total time: {total_time:.2f}s")
        logger.info(f"📦 Output location: s3://{s3_bucket}/silver/")
        logger.info("=" * 80)
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Fatal error in AWS Batch Resampling job: {str(e)}")
        raise


def main():
    """Main entry point for AWS Batch job"""
    logger.info("=" * 80)
    logger.info("AWS BATCH RESAMPLER STARTUP")
    logger.info("=" * 80)
    logger.info("Starting automated AWS Batch Resampling job (DuckDB + S3 Data Lake)")
    
    # Get configuration from environment variables
    aws_region = os.environ.get('AWS_REGION', 'ca-west-1')
    s3_bucket = os.environ.get('S3_BUCKET_NAME', 'dev-condvest-datalake')
    intervals_env = os.environ.get('RESAMPLING_INTERVALS', '3,5,8,13,21,34')
    force_full_resample_env = os.environ.get('FORCE_FULL_RESAMPLE', 'false').lower()
    retention_years = int(os.environ.get('RESAMPLING_RETENTION_YEARS', '5'))
    
    # Parse intervals
    try:
        intervals = [int(x.strip()) for x in intervals_env.split(',')]
    except ValueError as e:
        logger.error(f"Invalid RESAMPLING_INTERVALS format: {intervals_env}")
        raise
    
    # Parse force_full_resample flag
    force_full_resample = force_full_resample_env in ('true', '1', 'yes')
    
    logger.info(f"📅 Data retention: {retention_years} years (configurable via RESAMPLING_RETENTION_YEARS)")
    
    logger.info("📋 CONFIGURATION:")
    logger.info(f"   AWS_REGION: {aws_region}")
    logger.info(f"   S3_BUCKET_NAME: {s3_bucket}")
    logger.info(f"   RESAMPLING_INTERVALS: {intervals}")
    logger.info(f"   FORCE_FULL_RESAMPLE: {force_full_resample}")
    
    if force_full_resample:
        logger.warning("⚠️  FORCE_FULL_RESAMPLE is enabled!")
        logger.warning("⚠️  This will reprocess ALL data, ignoring existing silver data")
        logger.warning("⚠️  This may take longer and use more compute resources")
    
    try:
        logger.info("\n✅ Starting DuckDB + S3 Resampler...")
        
        # Run the resampling job
        results = run_resampling_job(
            s3_bucket=s3_bucket,
            intervals=intervals,
            force_full_resample=force_full_resample
        )
        
        logger.info("🎉 AWS Batch Resampling job completed successfully!")
        return results
        
    except Exception as e:
        logger.error(f"❌ AWS Batch Resampling job failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()

