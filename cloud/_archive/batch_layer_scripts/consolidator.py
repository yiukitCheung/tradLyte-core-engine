#!/usr/bin/env python3
"""
Bronze Layer Consolidation AWS Batch Job (PARALLEL PROCESSING)

Consolidates daily date=*.parquet files into single data.parquet per symbol.
Includes integrated vacuum (cleanup) of old date files after consolidation.
Uses ThreadPoolExecutor for parallel processing (5-8x faster than sequential).

Usage:
    AWS Batch:
        Environment Variables:
        - S3_BUCKET: dev-condvest-datalake
        - S3_PREFIX: bronze/raw_ohlcv
        - MODE: incremental or full
        - RETENTION_DAYS: 30
        - MAX_WORKERS: 10
        - SKIP_CLEANUP: false
        - SYMBOLS: AAPL,MSFT (optional, comma-separated)

    Local Testing:
        python consolidator.py --mode incremental --max-workers 10

Architecture:
    Lambda Fetcher → date=*.parquet (daily)
    This Job → data.parquet (consolidated) + cleanup old files
    Resampler (Batch) → silver layer
"""

import os
import sys
import json
import logging
import time
import argparse
import boto3
import duckdb
import concurrent.futures
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants (can be overridden by environment variables)
S3_BUCKET = os.environ.get('S3_BUCKET', 'dev-condvest-datalake')
S3_PREFIX = os.environ.get('S3_PREFIX', 'bronze/raw_ohlcv')
AWS_REGION = os.environ.get('AWS_REGION', 'ca-west-1')
RETENTION_DAYS = int(os.environ.get('RETENTION_DAYS', '30'))
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '10'))
METADATA_KEY = 'processing_metadata/consolidation_manifest.parquet'


class BronzeConsolidator:
    """
    AWS Batch Bronze Layer Consolidator with PARALLEL PROCESSING
    
    Features:
    - Incremental consolidation (only symbols with new data)
    - Integrated vacuum (cleanup old date files)
    - Uses RDS watermark for intelligent filtering
    - Explicit S3 paths (no slow wildcard scanning)
    - PARALLEL processing with ThreadPoolExecutor (5-8x faster!)
    """
    
    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str,
        aws_region: str,
        retention_days: int = 30,
        max_workers: int = 10
    ):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region
        self.retention_days = retention_days
        self.max_workers = max_workers
        
        # AWS clients
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
        # DuckDB connection (for main thread - parallel threads get their own)
        self.conn = self._init_duckdb()
        
        # Statistics
        self.stats = {
            'symbols_processed': 0,
            'symbols_consolidated': 0,
            'symbols_skipped': 0,
            'files_cleaned': 0,
            'bytes_freed': 0,
            'errors': [],
            'total_time_s': 0
        }
    
    def _init_duckdb(self) -> duckdb.DuckDBPyConnection:
        """Initialize DuckDB with S3 credentials"""
        conn = duckdb.connect(':memory:')
        
        # Install and load httpfs
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        
        # Set region
        conn.execute(f"SET s3_region='{self.aws_region}'")
        
        # Get credentials from environment or IAM role
        session = boto3.Session()
        credentials = session.get_credentials()
        
        if credentials:
            conn.execute(f"SET s3_access_key_id='{credentials.access_key}'")
            conn.execute(f"SET s3_secret_access_key='{credentials.secret_key}'")
            if credentials.token:
                conn.execute(f"SET s3_session_token='{credentials.token}'")
        
        return conn
    
    def _get_rds_connection(self) -> Optional[Dict]:
        """Get RDS connection from Secrets Manager"""
        secret_arn = os.environ.get('RDS_SECRET_ARN')
        if not secret_arn:
            logger.warning("RDS_SECRET_ARN not set, skipping watermark check")
            return None
        
        try:
            secrets_client = boto3.client('secretsmanager', region_name=self.aws_region)
            response = secrets_client.get_secret_value(SecretId=secret_arn)
            return json.loads(response['SecretString'])
        except Exception as e:
            logger.error(f"Failed to get RDS credentials: {e}")
            return None
    
    def _read_consolidation_metadata(self) -> Dict[str, Tuple[date, int]]:
        """Read consolidation metadata from S3"""
        try:
            s3_path = f"s3://{self.s3_bucket}/{METADATA_KEY}"
            df = self.conn.execute(f"""
                SELECT symbol, last_consolidated_date, row_count
                FROM read_parquet('{s3_path}')
            """).fetchdf()
            
            result = {}
            for _, row in df.iterrows():
                dt = row['last_consolidated_date']
                if hasattr(dt, 'date'):
                    dt = dt.date()
                row_count = int(row['row_count']) if row['row_count'] is not None else 0
                result[row['symbol']] = (dt, row_count)
            return result
        except Exception as e:
            logger.info(f"No consolidation metadata found (first run): {e}")
            return {}
    
    def _write_consolidation_metadata(self, metadata: Dict[str, Tuple[date, int]]):
        """Write consolidation metadata to S3"""
        import pandas as pd
        from io import BytesIO
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        records = [
            {
                'symbol': symbol,
                'last_consolidated_date': data[0],
                'row_count': data[1],
                'last_updated': datetime.now()
            }
            for symbol, data in metadata.items()
        ]
        
        df = pd.DataFrame(records)
        
        # Write to S3
        buffer = BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=METADATA_KEY,
            Body=buffer.getvalue()
        )
        
        logger.info(f"✅ Updated consolidation metadata for {len(records)} symbols")
    
    def _get_symbols_with_new_data(self) -> List[Dict]:
        """
        Get symbols that have new data since last consolidation.
        Uses RDS watermark table for intelligent filtering.
        """
        rds_creds = self._get_rds_connection()
        if not rds_creds:
            # Fallback: list all symbols from S3
            return self._list_all_symbols()
        
        # Read existing consolidation metadata
        consolidation_metadata = self._read_consolidation_metadata()
        
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host=rds_creds['host'],
                port=rds_creds.get('port', 5432),
                database=rds_creds['dbname'],
                user=rds_creds['username'],
                password=rds_creds['password'],
                sslmode='require'
            )
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT symbol, latest_date 
                FROM data_ingestion_watermark 
                WHERE is_current = TRUE
                ORDER BY symbol
            """)
            
            watermark_data = cursor.fetchall()
            conn.close()
            
            # Filter to symbols with new data
            symbols_to_process = []
            for symbol, watermark_date in watermark_data:
                last_consolidated = consolidation_metadata.get(symbol)
                
                if last_consolidated is None:
                    # Never consolidated - needs full consolidation
                    symbols_to_process.append({
                        'symbol': symbol,
                        'watermark_date': watermark_date,
                        'last_consolidated': None,
                        'mode': 'full'
                    })
                elif watermark_date > last_consolidated:
                    # Has new data - needs incremental consolidation
                    symbols_to_process.append({
                        'symbol': symbol,
                        'watermark_date': watermark_date,
                        'last_consolidated': last_consolidated,
                        'mode': 'incremental'
                    })
                # else: already up to date, skip
            
            logger.info(f"📊 Found {len(symbols_to_process)} symbols with new data (out of {len(watermark_data)} total)")
            return symbols_to_process
            
        except ImportError:
            logger.warning("psycopg2 not installed, falling back to S3 listing")
            return self._list_all_symbols()
        except Exception as e:
            logger.error(f"Failed to query watermark table: {e}")
            return self._list_all_symbols()
    
    def _list_all_symbols(self) -> List[Dict]:
        """Fallback: list all symbols from S3"""
        symbols = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(
            Bucket=self.s3_bucket,
            Prefix=f"{self.s3_prefix}/symbol=",
            Delimiter='/'
        ):
            for prefix in page.get('CommonPrefixes', []):
                symbol = prefix['Prefix'].split('symbol=')[1].rstrip('/')
                symbols.append({
                    'symbol': symbol,
                    'watermark_date': None,
                    'last_consolidated': None,
                    'mode': 'full'
                })
        
        return symbols
    
    def _list_date_files(self, symbol: str, after_date: Optional[date] = None) -> List[Dict]:
        """List date=*.parquet files for a symbol"""
        prefix = f"{self.s3_prefix}/symbol={symbol}/"
        date_files = []
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                filename = key.split('/')[-1]
                
                if filename.startswith('date=') and filename.endswith('.parquet'):
                    try:
                        date_str = filename.replace('date=', '').replace('.parquet', '')
                        file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        
                        # Filter by date if specified
                        if after_date and file_date <= after_date:
                            continue
                        
                        date_files.append({
                            'key': key,
                            'date': file_date,
                            'size': obj['Size']
                        })
                    except ValueError:
                        continue
        
        return sorted(date_files, key=lambda x: x['date'])
    
    def _check_data_parquet_exists(self, symbol: str) -> bool:
        """Check if data.parquet exists for a symbol"""
        key = f"{self.s3_prefix}/symbol={symbol}/data.parquet"
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=key)
            return True
        except:
            return False
    
    def _cleanup_old_date_files(self, symbol: str) -> Dict:
        """
        Clean up old date=*.parquet files for a symbol.
        Keeps only files from the last `retention_days` days.
        """
        cutoff_date = date.today() - timedelta(days=self.retention_days)
        date_files = self._list_date_files(symbol)
        
        files_to_delete = [f for f in date_files if f['date'] < cutoff_date]
        
        if not files_to_delete:
            return {'files_deleted': 0, 'bytes_freed': 0}
        
        # Delete files
        bytes_freed = 0
        files_deleted = 0
        
        for file_info in files_to_delete:
            try:
                self.s3_client.delete_object(
                    Bucket=self.s3_bucket,
                    Key=file_info['key']
                )
                files_deleted += 1
                bytes_freed += file_info['size']
            except Exception as e:
                logger.warning(f"Failed to delete {file_info['key']}: {e}")
        
        if files_deleted > 0:
            logger.info(f"  🗑️  Cleaned {files_deleted} old date files ({bytes_freed:,} bytes)")
        
        return {'files_deleted': files_deleted, 'bytes_freed': bytes_freed}
    
    def _consolidate_symbol_thread_safe(
        self,
        symbol: str,
        last_consolidated_date: Optional[date] = None,
        skip_cleanup: bool = False
    ) -> Dict:
        """
        Thread-safe consolidation for parallel processing.
        Each thread creates its own DuckDB connection.
        """
        # Create new DuckDB connection for this thread
        thread_conn = duckdb.connect(':memory:')
        thread_conn.execute("INSTALL httpfs")
        thread_conn.execute("LOAD httpfs")
        thread_conn.execute(f"SET s3_region='{self.aws_region}'")
        
        # Get credentials
        session = boto3.Session()
        credentials = session.get_credentials()
        
        if credentials:
            thread_conn.execute(f"SET s3_access_key_id='{credentials.access_key}'")
            thread_conn.execute(f"SET s3_secret_access_key='{credentials.secret_key}'")
            if credentials.token:
                thread_conn.execute(f"SET s3_session_token='{credentials.token}'")
        
        result = {
            'symbol': symbol,
            'status': 'success',
            'rows_consolidated': 0,
            'files_cleaned': 0,
            'bytes_freed': 0,
            'latest_date': None,
            'total_time_s': 0
        }
        
        total_start = time.time()
        
        try:
            s3_symbol_path = f"s3://{self.s3_bucket}/{self.s3_prefix}/symbol={symbol}"
            data_parquet_path = f"{s3_symbol_path}/data.parquet"
            
            # Get new date files to consolidate
            new_files = self._list_date_files(symbol, after_date=last_consolidated_date)
            
            if not new_files:
                result['status'] = 'skipped'
                result['reason'] = 'No new date files to consolidate'
                result['total_time_s'] = time.time() - total_start
                return result
            
            # Build explicit paths for new files
            new_file_paths = [f"s3://{self.s3_bucket}/{f['key']}" for f in new_files]
            paths_str = "', '".join(new_file_paths)
            
            # Standard columns to select (handles schema mismatches)
            STANDARD_COLUMNS = "symbol, open, high, low, close, volume, timestamp, interval"
            
            # Check if data.parquet exists
            has_existing_data = self._check_data_parquet_exists(symbol)
            
            if has_existing_data:
                # Use explicit columns to handle schema mismatch (e.g., __index_level_0__)
                merge_sql = f"""
                    SELECT {STANDARD_COLUMNS} FROM read_parquet('{data_parquet_path}')
                    UNION ALL
                    SELECT {STANDARD_COLUMNS} FROM read_parquet(['{paths_str}'])
                """
            else:
                all_files = self._list_date_files(symbol)
                if not all_files:
                    result['status'] = 'skipped'
                    result['reason'] = 'No date files found'
                    result['total_time_s'] = time.time() - total_start
                    return result
                
                all_paths = [f"s3://{self.s3_bucket}/{f['key']}" for f in all_files]
                paths_str = "', '".join(all_paths)
                merge_sql = f"SELECT {STANDARD_COLUMNS} FROM read_parquet(['{paths_str}'])"
            
            # Execute consolidation
            df = thread_conn.execute(merge_sql).fetchdf()
            df = df.drop_duplicates()
            
            # Write consolidated data to S3
            import pyarrow as pa
            import pyarrow.parquet as pq
            from io import BytesIO
            
            buffer = BytesIO()
            # preserve_index=False prevents adding __index_level_0__ column
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, buffer, compression='snappy')
            buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=f"{self.s3_prefix}/symbol={symbol}/data.parquet",
                Body=buffer.getvalue()
            )
            
            result['rows_consolidated'] = len(df)
            # Handle both column names for backward compatibility
            if 'timestamp' in df.columns:
                result['latest_date'] = df['timestamp'].max()
            elif 'timestamp_1' in df.columns:
                result['latest_date'] = df['timestamp_1'].max()
            else:
                result['latest_date'] = None
            
            # Cleanup old date files
            if not skip_cleanup:
                cleanup_result = self._cleanup_old_date_files(symbol)
                result['files_cleaned'] = cleanup_result['files_deleted']
                result['bytes_freed'] = cleanup_result['bytes_freed']
            
            result['total_time_s'] = time.time() - total_start
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            result['total_time_s'] = time.time() - total_start
        finally:
            thread_conn.close()
        
        return result
    
    def _process_symbols_parallel(
        self,
        symbols_to_process: List[Dict],
        skip_cleanup: bool = False
    ) -> List[Dict]:
        """
        Process multiple symbols in parallel using ThreadPoolExecutor.
        """
        results = []
        
        logger.info(f"🚀 Starting PARALLEL consolidation with {self.max_workers} workers...")
        
        def process_symbol(symbol_info: Dict) -> Dict:
            """Worker function for each symbol"""
            return self._consolidate_symbol_thread_safe(
                symbol=symbol_info['symbol'],
                last_consolidated_date=symbol_info.get('last_consolidated'),
                skip_cleanup=skip_cleanup
            )
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(process_symbol, sym_info): sym_info['symbol']
                for sym_info in symbols_to_process
            }
            
            # Collect results as they complete
            completed = 0
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                completed += 1
                try:
                    result = future.result()
                    results.append(result)
                    
                    status_icon = "✅" if result['status'] == 'success' else "⏭️" if result['status'] == 'skipped' else "❌"
                    logger.info(f"[{completed}/{len(symbols_to_process)}] {status_icon} {symbol}: {result['rows_consolidated']:,} rows in {result.get('total_time_s', 0):.1f}s")
                    
                except Exception as e:
                    logger.error(f"[{completed}/{len(symbols_to_process)}] ❌ {symbol}: {e}")
                    results.append({
                        'symbol': symbol,
                        'status': 'failed',
                        'error': str(e)
                    })
        
        return results
    
    def run(
        self,
        mode: str = 'incremental',
        symbols: Optional[List[str]] = None,
        skip_cleanup: bool = False
    ) -> Dict:
        """
        Run consolidation job with PARALLEL PROCESSING
        """
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info("🔧 BRONZE LAYER CONSOLIDATION (AWS BATCH - PARALLEL)")
        logger.info("=" * 60)
        logger.info(f"Mode: {mode}")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Bucket: {self.s3_bucket}")
        logger.info(f"Prefix: {self.s3_prefix}")
        logger.info(f"Retention: {self.retention_days} days")
        logger.info(f"Skip cleanup: {skip_cleanup}")
        
        # Get symbols to process
        if symbols:
            symbols_to_process = [
                {'symbol': s, 'watermark_date': None, 'last_consolidated': None, 'mode': mode}
                for s in symbols
            ]
        elif mode == 'incremental':
            symbols_to_process = self._get_symbols_with_new_data()
        else:
            symbols_to_process = self._list_all_symbols()
        
        if not symbols_to_process:
            logger.info("✅ No symbols need consolidation")
            return {'status': 'success', 'symbols_processed': 0, 'message': 'No work needed'}
        
        logger.info(f"📋 Processing {len(symbols_to_process)} symbols")
        
        # Read existing metadata
        metadata = self._read_consolidation_metadata()
        
        # PARALLEL PROCESSING
        results = self._process_symbols_parallel(
            symbols_to_process=symbols_to_process,
            skip_cleanup=skip_cleanup
        )
        
        # Update stats from results
        for result in results:
            self.stats['symbols_processed'] += 1
            
            if result['status'] == 'success':
                self.stats['symbols_consolidated'] += 1
                self.stats['files_cleaned'] += result.get('files_cleaned', 0)
                self.stats['bytes_freed'] += result.get('bytes_freed', 0)
                
                # Update metadata
                if result.get('latest_date'):
                    latest = result['latest_date']
                    if hasattr(latest, 'date'):
                        latest = latest.date()
                    metadata[result['symbol']] = (latest, result['rows_consolidated'])
                    
            elif result['status'] == 'skipped':
                self.stats['symbols_skipped'] += 1
            else:
                self.stats['errors'].append({'symbol': result['symbol'], 'error': result.get('error')})
        
        # Write updated metadata
        if metadata:
            self._write_consolidation_metadata(metadata)
        
        total_time = time.time() - start_time
        self.stats['total_time_s'] = total_time
        
        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("📊 CONSOLIDATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total time: {total_time:.2f}s ({total_time/60:.1f} min)")
        logger.info(f"Throughput: {len(results)/total_time:.1f} symbols/sec")
        logger.info(f"Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"Symbols consolidated: {self.stats['symbols_consolidated']}")
        logger.info(f"Symbols skipped: {self.stats['symbols_skipped']}")
        logger.info(f"Files cleaned: {self.stats['files_cleaned']}")
        logger.info(f"Space freed: {self.stats['bytes_freed']:,} bytes ({self.stats['bytes_freed'] / (1024*1024):.2f} MB)")
        logger.info(f"Errors: {len(self.stats['errors'])}")
        
        return {
            'status': 'success' if not self.stats['errors'] else 'partial',
            'stats': self.stats
        }


def main():
    """Main entry point for AWS Batch job"""
    logger.info("=" * 80)
    logger.info("AWS BATCH CONSOLIDATOR STARTUP")
    logger.info("=" * 80)
    logger.info("Starting automated AWS Batch Consolidation job")
    
    # Parse command line arguments (for local testing)
    parser = argparse.ArgumentParser(description='Bronze Layer Consolidation Job')
    parser.add_argument('--mode', type=str, default=os.environ.get('MODE', 'incremental'),
                        choices=['incremental', 'full'], help='Consolidation mode')
    parser.add_argument('--symbols', type=str, default=os.environ.get('SYMBOLS', ''),
                        help='Comma-separated list of symbols to process')
    parser.add_argument('--retention-days', type=int, 
                        default=int(os.environ.get('RETENTION_DAYS', '30')),
                        help='Days to keep date files')
    parser.add_argument('--max-workers', type=int,
                        default=int(os.environ.get('MAX_WORKERS', '10')),
                        help='Number of parallel workers')
    parser.add_argument('--skip-cleanup', action='store_true',
                        default=os.environ.get('SKIP_CLEANUP', 'false').lower() == 'true',
                        help='Skip vacuum after consolidation')
    
    args = parser.parse_args()
    
    # Parse symbols
    symbols = [s.strip() for s in args.symbols.split(',') if s.strip()] or None
    
    # Get configuration
    s3_bucket = os.environ.get('S3_BUCKET', 'dev-condvest-datalake')
    s3_prefix = os.environ.get('S3_PREFIX', 'bronze/raw_ohlcv')
    aws_region = os.environ.get('AWS_REGION', 'ca-west-1')
    
    logger.info("📋 CONFIGURATION:")
    logger.info(f"   S3_BUCKET: {s3_bucket}")
    logger.info(f"   S3_PREFIX: {s3_prefix}")
    logger.info(f"   AWS_REGION: {aws_region}")
    logger.info(f"   MODE: {args.mode}")
    logger.info(f"   SYMBOLS: {symbols if symbols else 'ALL'}")
    logger.info(f"   RETENTION_DAYS: {args.retention_days}")
    logger.info(f"   MAX_WORKERS: {args.max_workers}")
    logger.info(f"   SKIP_CLEANUP: {args.skip_cleanup}")
    
    try:
        # Initialize consolidator
        consolidator = BronzeConsolidator(
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            aws_region=aws_region,
            retention_days=args.retention_days,
            max_workers=args.max_workers
        )
        
        # Run consolidation
        result = consolidator.run(
            mode=args.mode,
            symbols=symbols,
            skip_cleanup=args.skip_cleanup
        )
        
        logger.info("=" * 80)
        if result['status'] == 'success':
            logger.info("🎉 AWS Batch Consolidation job completed successfully!")
        else:
            logger.warning("⚠️  AWS Batch Consolidation job completed with some errors")
        logger.info("=" * 80)
        
        return result
        
    except Exception as e:
        logger.error(f"❌ AWS Batch Consolidation job failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()

