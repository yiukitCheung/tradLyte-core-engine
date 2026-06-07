#!/usr/bin/env python3
"""
Bronze Layer Cleanup Script (with PARALLEL PROCESSING)

This script cleans up old date=*.parquet files in the bronze layer:

1. For symbols WITH data.parquet:
   - Delete date=*.parquet files older than 30 days
   - Keep recent 30 days of date=*.parquet files (as safety buffer)
   
2. For symbols WITHOUT data.parquet:
   - Don't touch them (all date=*.parquet files preserved)

3. Maintains a cleanup metadata file in S3 to track what's been cleaned

4. Uses PARALLEL PROCESSING by default (5-8x faster than sequential!)

Usage:
    # Parallel mode (default, 5-8x faster!)
    python vacuum.py [--dry-run] [--retention-days 30] [--max-workers 10]
    
    # Sequential mode (for debugging)
    python vacuum.py --sequential [--dry-run]

Arguments:
    --dry-run           Show what would be deleted without actually deleting
    --retention-days    Number of days to keep (default: 30)
    --symbols           Comma-separated list of symbols to process (optional)
    --max-workers       Number of parallel workers (default: 10)
    --sequential        Use sequential processing instead of parallel
"""

import os
import sys
import logging
import argparse
import boto3
import duckdb
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeLayerCleaner:
    """
    Cleans up old date=*.parquet files in the bronze layer
    while preserving consolidated data.parquet files.
    """
    
    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str = "bronze/raw_ohlcv",
        aws_region: str = "ca-west-1",
        retention_days: int = 30,
        dry_run: bool = False
    ):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region
        self.retention_days = retention_days
        self.dry_run = dry_run
        
        # S3 client
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
        # Metadata paths
        self.metadata_prefix = "processing_metadata"
        self.cleanup_metadata_key = f"{self.metadata_prefix}/cleanup_manifest.json"
        
        # DuckDB connection for S3 access
        self.conn = self._init_duckdb()
        
        # Statistics
        self.stats = {
            'symbols_processed': 0,
            'symbols_with_data_parquet': 0,
            'symbols_without_data_parquet': 0,
            'files_deleted': 0,
            'files_kept': 0,
            'bytes_freed': 0,
            'errors': []
        }
    
    def _init_duckdb(self) -> duckdb.DuckDBPyConnection:
        """Initialize DuckDB with S3 credentials"""
        conn = duckdb.connect()
        
        # Configure S3 access
        aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
        conn.execute(f"SET s3_region='{self.aws_region}'")
        
        if aws_access_key and aws_secret_key:
            conn.execute(f"SET s3_access_key_id='{aws_access_key}'")
            conn.execute(f"SET s3_secret_access_key='{aws_secret_key}'")
        
        return conn
    
    def _read_cleanup_metadata(self) -> Dict:
        """Read cleanup metadata from S3"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=self.cleanup_metadata_key
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3_client.exceptions.NoSuchKey:
            logger.info("ℹ️  No cleanup metadata found (first run)")
            return {
                'last_cleanup': None,
                'symbols_cleaned': {},
                'total_files_deleted': 0,
                'total_bytes_freed': 0
            }
        except Exception as e:
            logger.warning(f"⚠️  Error reading cleanup metadata: {e}")
            return {
                'last_cleanup': None,
                'symbols_cleaned': {},
                'total_files_deleted': 0,
                'total_bytes_freed': 0
            }
    
    def _write_cleanup_metadata(self, metadata: Dict):
        """Write cleanup metadata to S3"""
        if self.dry_run:
            logger.info("🔍 [DRY RUN] Would write cleanup metadata")
            return
        
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.cleanup_metadata_key,
                Body=json.dumps(metadata, indent=2, default=str),
                ContentType='application/json'
            )
            logger.info(f"✅ Updated cleanup metadata: s3://{self.s3_bucket}/{self.cleanup_metadata_key}")
        except Exception as e:
            logger.error(f"❌ Error writing cleanup metadata: {e}")
    
    def list_symbols(self) -> List[str]:
        """List all symbols in the bronze layer"""
        logger.info(f"📋 Listing symbols from s3://{self.s3_bucket}/{self.s3_prefix}/...")
        
        symbols = set()
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(
            Bucket=self.s3_bucket,
            Prefix=f"{self.s3_prefix}/symbol=",
            Delimiter='/'
        ):
            for prefix in page.get('CommonPrefixes', []):
                # Extract symbol from prefix like "bronze/raw_ohlcv/symbol=AAPL/"
                prefix_str = prefix['Prefix']
                symbol = prefix_str.split('symbol=')[1].rstrip('/')
                symbols.add(symbol)
        
        symbols_list = sorted(list(symbols))
        logger.info(f"✅ Found {len(symbols_list)} symbols")
        return symbols_list
    
    def _check_data_parquet_exists(self, symbol: str) -> bool:
        """Check if data.parquet exists for a symbol"""
        key = f"{self.s3_prefix}/symbol={symbol}/data.parquet"
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=key)
            return True
        except:
            return False
    
    def _get_data_parquet_latest_date(self, symbol: str) -> Optional[date]:
        """Get the latest date from data.parquet for a symbol"""
        try:
            s3_path = f"s3://{self.s3_bucket}/{self.s3_prefix}/symbol={symbol}/data.parquet"
            # Try 'timestamp' first (current standard), fall back to 'timestamp_1' (legacy)
            try:
                result = self.conn.execute(f"""
                    SELECT MAX(CAST(timestamp AS DATE)) as latest_date
                    FROM read_parquet('{s3_path}')
                """).fetchone()
            except:
                # Fallback to legacy column name
                result = self.conn.execute(f"""
                    SELECT MAX(CAST(timestamp_1 AS DATE)) as latest_date
                    FROM read_parquet('{s3_path}')
                """).fetchone()
            
            if result and result[0]:
                return result[0]
            return None
        except Exception as e:
            logger.warning(f"⚠️  Could not read latest date from data.parquet for {symbol}: {e}")
            return None
    
    def _list_date_files(self, symbol: str) -> List[Dict]:
        """List all date=*.parquet files for a symbol with their metadata"""
        prefix = f"{self.s3_prefix}/symbol={symbol}/"
        date_files = []
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                filename = key.split('/')[-1]
                
                # Only process date=*.parquet files
                if filename.startswith('date=') and filename.endswith('.parquet'):
                    try:
                        # Extract date from filename like "date=2025-11-28.parquet"
                        date_str = filename.replace('date=', '').replace('.parquet', '')
                        file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        
                        date_files.append({
                            'key': key,
                            'date': file_date,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
                    except ValueError:
                        # Skip files that don't match the expected format
                        continue
        
        return date_files
    
    def cleanup_symbol(self, symbol: str) -> Dict:
        """
        Clean up old date=*.parquet files for a single symbol
        
        Returns:
            Dict with cleanup results
        """
        result = {
            'symbol': symbol,
            'status': 'skipped',
            'has_data_parquet': False,
            'files_deleted': 0,
            'files_kept': 0,
            'bytes_freed': 0,
            'deleted_files': [],
            'kept_files': []
        }
        
        # Check if data.parquet exists
        has_data_parquet = self._check_data_parquet_exists(symbol)
        result['has_data_parquet'] = has_data_parquet
        
        if not has_data_parquet:
            # Don't touch symbols without data.parquet
            result['status'] = 'skipped'
            result['reason'] = 'No data.parquet found - preserving all date files'
            return result
        
        # Get all date=*.parquet files
        date_files = self._list_date_files(symbol)
        
        if not date_files:
            result['status'] = 'success'
            result['reason'] = 'No date=*.parquet files to clean'
            return result
        
        # Calculate cutoff date (keep files from last N days)
        today = date.today()
        cutoff_date = today - timedelta(days=self.retention_days)
        
        # Categorize files
        files_to_delete = []
        files_to_keep = []
        
        for file_info in date_files:
            if file_info['date'] < cutoff_date:
                files_to_delete.append(file_info)
            else:
                files_to_keep.append(file_info)
        
        result['files_kept'] = len(files_to_keep)
        result['kept_files'] = [f['key'] for f in files_to_keep]
        
        if not files_to_delete:
            result['status'] = 'success'
            result['reason'] = f'All {len(files_to_keep)} date files are within retention period'
            return result
        
        # Delete old files
        bytes_freed = 0
        deleted_count = 0
        
        for file_info in files_to_delete:
            if self.dry_run:
                logger.info(f"  🔍 [DRY RUN] Would delete: {file_info['key']} ({file_info['date']}, {file_info['size']:,} bytes)")
                deleted_count += 1
                bytes_freed += file_info['size']
            else:
                try:
                    self.s3_client.delete_object(
                        Bucket=self.s3_bucket,
                        Key=file_info['key']
                    )
                    deleted_count += 1
                    bytes_freed += file_info['size']
                    result['deleted_files'].append(file_info['key'])
                except Exception as e:
                    logger.error(f"  ❌ Failed to delete {file_info['key']}: {e}")
        
        result['files_deleted'] = deleted_count
        result['bytes_freed'] = bytes_freed
        result['status'] = 'success'
        
        return result
    
    def cleanup_all(
        self,
        symbols: Optional[List[str]] = None,
        max_workers: int = 10,
        use_parallel: bool = True
    ) -> Dict:
        """
        Clean up old date=*.parquet files for all symbols
        
        Args:
            symbols: Optional list of specific symbols to process
            max_workers: Number of parallel workers for cleanup
            use_parallel: If True, use parallel processing (5-8x faster!)
            
        Returns:
            Dict with overall cleanup statistics
        """
        import time
        start_time = time.time()
        
        logger.info("=" * 80)
        logger.info("🧹 BRONZE LAYER CLEANUP" + (" (PARALLEL)" if use_parallel else " (SEQUENTIAL)"))
        logger.info("=" * 80)
        
        if self.dry_run:
            logger.info("🔍 DRY RUN MODE - No files will be deleted")
        
        logger.info(f"📋 Configuration:")
        logger.info(f"   S3 Bucket: {self.s3_bucket}")
        logger.info(f"   S3 Prefix: {self.s3_prefix}")
        logger.info(f"   Retention Days: {self.retention_days}")
        if use_parallel:
            logger.info(f"   Max Workers: {max_workers}")
        logger.info("")
        
        # Read existing metadata
        metadata = self._read_cleanup_metadata()
        
        # Get symbols to process
        if symbols:
            symbols_to_process = symbols
            logger.info(f"🎯 Processing specified symbols: {len(symbols_to_process)}")
        else:
            symbols_to_process = self.list_symbols()
            logger.info(f"🎯 Processing all symbols: {len(symbols_to_process)}")
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("📂 PROCESSING SYMBOLS")
        logger.info("=" * 80)
        
        results = []
        
        if use_parallel:
            # PARALLEL PROCESSING - 5-8x faster!
            logger.info(f"🚀 Starting PARALLEL cleanup with {max_workers} workers...")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_symbol = {
                    executor.submit(self.cleanup_symbol, symbol): symbol
                    for symbol in symbols_to_process
                }
                
                # Collect results as they complete
                completed = 0
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    completed += 1
                    
                    try:
                        result = future.result()
                        results.append(result)
                        
                        self.stats['symbols_processed'] += 1
                        
                        if result['has_data_parquet']:
                            self.stats['symbols_with_data_parquet'] += 1
                        else:
                            self.stats['symbols_without_data_parquet'] += 1
                        
                        self.stats['files_deleted'] += result['files_deleted']
                        self.stats['files_kept'] += result['files_kept']
                        self.stats['bytes_freed'] += result['bytes_freed']
                        
                        # Log result
                        if result['status'] == 'skipped':
                            logger.info(f"[{completed}/{len(symbols_to_process)}] ⏭️  {symbol}: Skipped")
                        elif result['files_deleted'] > 0:
                            logger.info(f"[{completed}/{len(symbols_to_process)}] 🗑️  {symbol}: Deleted {result['files_deleted']} files ({result['bytes_freed']:,} bytes)")
                        else:
                            logger.info(f"[{completed}/{len(symbols_to_process)}] ✅ {symbol}: Clean")
                        
                        # Update metadata for this symbol
                        if result['status'] == 'success' and result['files_deleted'] > 0:
                            metadata['symbols_cleaned'][symbol] = {
                                'last_cleanup': datetime.now().isoformat(),
                                'files_deleted': result['files_deleted'],
                                'bytes_freed': result['bytes_freed']
                            }
                            
                    except Exception as e:
                        logger.error(f"[{completed}/{len(symbols_to_process)}] ❌ {symbol}: {e}")
                        results.append({
                            'symbol': symbol,
                            'status': 'failed',
                            'error': str(e),
                            'has_data_parquet': False,
                            'files_deleted': 0,
                            'files_kept': 0,
                            'bytes_freed': 0
                        })
        else:
            # SEQUENTIAL PROCESSING (original behavior)
            for i, symbol in enumerate(symbols_to_process, 1):
                logger.info(f"[{i}/{len(symbols_to_process)}] Processing {symbol}...")
                
                result = self.cleanup_symbol(symbol)
                results.append(result)
                
                self.stats['symbols_processed'] += 1
                
                if result['has_data_parquet']:
                    self.stats['symbols_with_data_parquet'] += 1
                else:
                    self.stats['symbols_without_data_parquet'] += 1
                
                self.stats['files_deleted'] += result['files_deleted']
                self.stats['files_kept'] += result['files_kept']
                self.stats['bytes_freed'] += result['bytes_freed']
                
                # Log result
                if result['status'] == 'skipped':
                    logger.info(f"  ⏭️  Skipped: {result.get('reason', 'Unknown')}")
                else:
                    if result['files_deleted'] > 0:
                        logger.info(f"  🗑️  Deleted: {result['files_deleted']} files ({result['bytes_freed']:,} bytes)")
                        logger.info(f"  📁 Kept: {result['files_kept']} files (within {self.retention_days} days)")
                    else:
                        logger.info(f"  ✅ Clean: {result.get('reason', 'No old files')}")
                
                # Update metadata for this symbol
                if result['status'] == 'success' and result['files_deleted'] > 0:
                    metadata['symbols_cleaned'][symbol] = {
                        'last_cleanup': datetime.now().isoformat(),
                        'files_deleted': result['files_deleted'],
                        'bytes_freed': result['bytes_freed']
                    }
        
        total_time = time.time() - start_time
        
        # Update overall metadata
        metadata['last_cleanup'] = datetime.now().isoformat()
        metadata['total_files_deleted'] = metadata.get('total_files_deleted', 0) + self.stats['files_deleted']
        metadata['total_bytes_freed'] = metadata.get('total_bytes_freed', 0) + self.stats['bytes_freed']
        
        # Save metadata
        self._write_cleanup_metadata(metadata)
        
        # Print summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("📊 CLEANUP SUMMARY")
        logger.info("=" * 80)
        logger.info(f"{'DRY RUN - ' if self.dry_run else ''}Cleanup completed!")
        logger.info(f"   Total time: {total_time:.2f}s ({total_time/60:.1f} min)")
        logger.info(f"   Throughput: {len(symbols_to_process)/total_time:.1f} symbols/sec")
        logger.info(f"   Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"   With data.parquet: {self.stats['symbols_with_data_parquet']}")
        logger.info(f"   Without data.parquet (skipped): {self.stats['symbols_without_data_parquet']}")
        logger.info(f"   Files deleted: {self.stats['files_deleted']}")
        logger.info(f"   Files kept: {self.stats['files_kept']}")
        logger.info(f"   Space freed: {self.stats['bytes_freed']:,} bytes ({self.stats['bytes_freed'] / (1024*1024):.2f} MB)")
        logger.info("=" * 80)
        
        return {
            'status': 'success',
            'dry_run': self.dry_run,
            'total_time_s': total_time,
            'stats': self.stats,
            'results': results
        }


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Clean up old date=*.parquet files in the bronze layer'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    parser.add_argument(
        '--retention-days',
        type=int,
        default=30,
        help='Number of days to keep date=*.parquet files (default: 30)'
    )
    parser.add_argument(
        '--symbols',
        type=str,
        help='Comma-separated list of symbols to process (optional)'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        default=os.environ.get('S3_BUCKET', 'dev-condvest-datalake'),
        help='S3 bucket name'
    )
    parser.add_argument(
        '--prefix',
        type=str,
        default=os.environ.get('S3_PREFIX', 'bronze/raw_ohlcv'),
        help='S3 prefix for bronze layer'
    )
    parser.add_argument(
        '--region',
        type=str,
        default=os.environ.get('AWS_REGION', 'ca-west-1'),
        help='AWS region'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        default=True,
        help='Use parallel processing (default: True, 5-8x faster!)'
    )
    parser.add_argument(
        '--sequential',
        action='store_true',
        help='Use sequential processing (slower, but easier to debug)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=10,
        help='Number of parallel workers (default: 10)'
    )
    
    args = parser.parse_args()
    
    # Parse symbols if provided
    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    
    # Determine parallel mode
    use_parallel = not args.sequential  # Default to parallel unless --sequential is specified
    
    # Create cleaner and run
    cleaner = BronzeLayerCleaner(
        s3_bucket=args.bucket,
        s3_prefix=args.prefix,
        aws_region=args.region,
        retention_days=args.retention_days,
        dry_run=args.dry_run
    )
    
    try:
        result = cleaner.cleanup_all(
            symbols=symbols,
            max_workers=args.max_workers,
            use_parallel=use_parallel
        )
        
        # Exit with appropriate code
        if result['status'] == 'success':
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Cleanup failed with error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

