#!/usr/bin/env python3
"""
Gap Data Recovery Script
========================
Exports RDS data for a specified date range to S3 date=*.parquet files.

Usage:
    python recover_gap_data.py --start 2025-10-03 --end 2025-11-10 --workers 20
    python recover_gap_data.py --start 2025-10-03 --end 2025-11-10 --dry-run

Features:
    - Single RDS query for efficiency
    - Parallel S3 writes using ThreadPoolExecutor
    - Progress bar with tqdm
    - Detailed logging and error handling
"""

import os
import sys
import time
import argparse
import threading
from io import BytesIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================
# Configuration
# ============================================
S3_BUCKET = os.environ.get('S3_BUCKET', 'dev-condvest-datalake')
S3_PREFIX = os.environ.get('S3_PREFIX', 'bronze/raw_ohlcv')
AWS_REGION = os.environ.get('AWS_REGION', 'ca-west-1')

# RDS Configuration
RDS_HOST = os.environ.get('RDS_HOST')
RDS_PORT = os.environ.get('RDS_PORT', '5432')
RDS_DATABASE = os.environ.get('RDS_DATABASE')
RDS_USER = os.environ.get('RDS_USER')
RDS_PASSWORD = os.environ.get('RDS_PASSWORD')

# Thread-local storage for S3 clients
thread_local = threading.local()


def get_s3_client():
    """Get thread-local S3 client for thread safety"""
    if not hasattr(thread_local, 's3_client'):
        thread_local.s3_client = boto3.client('s3', region_name=AWS_REGION)
    return thread_local.s3_client


def get_rds_connection():
    """Create RDS connection"""
    return psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        database=RDS_DATABASE,
        user=RDS_USER,
        password=RDS_PASSWORD,
        sslmode='require'
    )


def fetch_gap_data(conn, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch all OHLCV data for the gap period from RDS.
    
    Args:
        conn: PostgreSQL connection
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        DataFrame with gap data
    """
    print(f"\n📥 Fetching RDS data: {start_date} to {end_date}")
    
    query = f"""
        SELECT 
            symbol,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            interval
        FROM raw_ohlcv 
        WHERE DATE(timestamp) BETWEEN '{start_date}' AND '{end_date}'
          AND interval = '1d'
        ORDER BY symbol, timestamp
    """
    
    start_time = time.time()
    df = pd.read_sql(query, conn)
    elapsed = time.time() - start_time
    
    print(f"✅ Fetched {len(df):,} records in {elapsed:.1f}s")
    print(f"   Symbols: {df['symbol'].nunique():,}")
    print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    return df


def prepare_batches(df: pd.DataFrame) -> list:
    """
    Group data by (symbol, date) for batch processing.
    
    Args:
        df: DataFrame with OHLCV data
    
    Returns:
        List of batch dictionaries
    """
    print(f"\n📦 Preparing batches...")
    
    # Add date column for grouping
    df['date'] = df['timestamp'].dt.date
    
    # Group by symbol and date
    batches = []
    for (symbol, date_val), group_df in df.groupby(['symbol', 'date']):
        batches.append({
            'symbol': symbol,
            'date': date_val,
            'df': group_df.drop(columns=['date'])
        })
    
    print(f"✅ Created {len(batches):,} batches")
    
    return batches


def write_batch_to_s3(batch: dict, dry_run: bool = False) -> dict:
    """
    Write a single (symbol, date) batch to S3.
    
    Args:
        batch: Dict with 'symbol', 'date', 'df' keys
        dry_run: If True, don't actually write
    
    Returns:
        Result dict with success/error info
    """
    symbol = batch['symbol']
    date_val = batch['date']
    df = batch['df']
    
    try:
        if dry_run:
            return {
                'symbol': symbol,
                'date': str(date_val),
                'success': True,
                'error': None,
                'dry_run': True
            }
        
        s3_client = get_s3_client()
        
        # Keep timestamp column name as-is (matches Lambda fetcher format)
        df_copy = df.copy()
        # Don't rename - Lambda fetcher uses 'timestamp', not 'timestamp_1'
        
        # Convert to PyArrow table and write to buffer
        table = pa.Table.from_pandas(df_copy, preserve_index=False)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        # S3 key: bronze/raw_ohlcv/symbol=AAPL/date=2025-10-03.parquet
        s3_key = f"{S3_PREFIX}/symbol={symbol}/date={date_val}.parquet"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue()
        )
        
        return {
            'symbol': symbol,
            'date': str(date_val),
            'success': True,
            'error': None
        }
        
    except Exception as e:
        return {
            'symbol': symbol,
            'date': str(date_val),
            'success': False,
            'error': str(e)
        }


def execute_parallel_upload(batches: list, max_workers: int, dry_run: bool = False):
    """
    Execute parallel upload of all batches to S3.
    
    Args:
        batches: List of batch dictionaries
        max_workers: Number of parallel workers
        dry_run: If True, don't actually write
    """
    print(f"\n🚀 {'[DRY RUN] ' if dry_run else ''}Starting parallel upload...")
    print(f"   Batches: {len(batches):,}")
    print(f"   Workers: {max_workers}")
    print("=" * 60)
    
    start_time = time.time()
    
    successful = 0
    failed = 0
    errors = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(write_batch_to_s3, batch, dry_run): batch 
            for batch in batches
        }
        
        # Process with progress bar
        with tqdm(total=len(batches), desc="Uploading", unit="files") as pbar:
            for future in as_completed(futures):
                result = future.result()
                
                if result['success']:
                    successful += 1
                else:
                    failed += 1
                    errors.append(result)
                
                pbar.update(1)
    
    elapsed = time.time() - start_time
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Upload Summary")
    print("=" * 60)
    print(f"✅ Successful: {successful:,}")
    print(f"❌ Failed: {failed:,}")
    print(f"⏱️  Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"📈 Rate: {successful / elapsed:.0f} files/sec")
    
    if errors:
        print(f"\n⚠️ Errors ({len(errors)}):")
        for err in errors[:10]:
            print(f"   {err['symbol']} ({err['date']}): {err['error']}")
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more")
    
    return successful, failed, errors


def verify_upload(start_date: str, end_date: str):
    """Verify uploaded files with spot checks"""
    print(f"\n🔍 Verifying uploads...")
    
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    
    # Check sample files
    sample_checks = [
        ('AAPL', start_date),
        ('MSFT', end_date),
        ('GOOGL', start_date),
    ]
    
    for symbol, date_str in sample_checks:
        s3_key = f"{S3_PREFIX}/symbol={symbol}/date={date_str}.parquet"
        
        try:
            response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            size_kb = response['ContentLength'] / 1024
            print(f"✅ {symbol} ({date_str}): {size_kb:.1f} KB")
        except Exception as e:
            print(f"❌ {symbol} ({date_str}): NOT FOUND")


def main():
    parser = argparse.ArgumentParser(
        description='Recover gap data from RDS to S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python recover_gap_data.py --start 2025-10-03 --end 2025-11-10
    python recover_gap_data.py --start 2025-10-03 --end 2025-11-10 --workers 30
    python recover_gap_data.py --start 2025-10-03 --end 2025-11-10 --dry-run
        """
    )
    
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--workers', type=int, default=20, help='Number of parallel workers (default: 20)')
    parser.add_argument('--dry-run', action='store_true', help='Simulate without writing to S3')
    parser.add_argument('--skip-verify', action='store_true', help='Skip verification step')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔧 Gap Data Recovery Script")
    print("=" * 60)
    print(f"📅 Date range: {args.start} to {args.end}")
    print(f"📦 S3 Bucket: {S3_BUCKET}")
    print(f"📂 S3 Prefix: {S3_PREFIX}")
    print(f"👷 Workers: {args.workers}")
    print(f"🧪 Dry run: {args.dry_run}")
    
    # Validate dates
    try:
        datetime.strptime(args.start, '%Y-%m-%d')
        datetime.strptime(args.end, '%Y-%m-%d')
    except ValueError as e:
        print(f"❌ Invalid date format: {e}")
        sys.exit(1)
    
    # Check RDS credentials
    if not all([RDS_HOST, RDS_DATABASE, RDS_USER, RDS_PASSWORD]):
        print("❌ Missing RDS credentials in environment variables")
        print("   Required: RDS_HOST, RDS_DATABASE, RDS_USER, RDS_PASSWORD")
        sys.exit(1)
    
    total_start = time.time()
    
    try:
        # Step 1: Connect to RDS
        print(f"\n🔌 Connecting to RDS...")
        conn = get_rds_connection()
        print(f"✅ Connected to {RDS_HOST}")
        
        # Step 2: Fetch gap data
        df = fetch_gap_data(conn, args.start, args.end)
        
        if df.empty:
            print("⚠️ No data found for the specified date range!")
            sys.exit(0)
        
        # Step 3: Prepare batches
        batches = prepare_batches(df)
        
        # Step 4: Execute parallel upload
        successful, failed, errors = execute_parallel_upload(
            batches, 
            args.workers, 
            args.dry_run
        )
        
        # Step 5: Verify (unless skipped or dry run)
        if not args.skip_verify and not args.dry_run:
            verify_upload(args.start, args.end)
        
        # Close connection
        conn.close()
        
        total_elapsed = time.time() - total_start
        
        print("\n" + "=" * 60)
        print("🎉 Recovery Complete!")
        print("=" * 60)
        print(f"⏱️  Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
        print(f"\n📋 Next steps:")
        print(f"   1. Run consolidator to merge new date files into data.parquet:")
        print(f"      python consolidator.py --mode full --max-workers 10")
        print(f"   2. Run resampler to update silver layer:")
        print(f"      python resampler.py")
        
        if failed > 0:
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

