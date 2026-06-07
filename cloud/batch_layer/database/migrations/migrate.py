#!/usr/bin/env python3
"""
FAST Database Migration Script: Local Docker PostgreSQL -> AWS RDS PostgreSQL

Optimized for LARGE datasets (millions of rows)
Uses advanced techniques: COPY, index dropping, parallel processing

Usage:
    python migrate_fast.py --use-test-tables
    python migrate_fast.py --use-test-tables --skip-indexes  # Even faster!
    python migrate_fast.py --use-test-tables --method copy   # Fastest!
"""

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import boto3
import json
import logging
import argparse
import os
import io
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from decimal import Decimal
import csv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_fast_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FastDatabaseMigration:
    """High-performance migration optimized for millions of rows"""
    
    TABLES = [
        'symbol_metadata',
        'raw_ohlcv',
        'silver_3d',
        'silver_5d',
        'silver_8d',
        'silver_13d',
        'silver_21d',
        'silver_34d'
    ]
    
    def __init__(self, use_test_tables: bool = False, skip_indexes: bool = False, method: str = 'bulk'):
        """
        Initialize fast migration
        
        Args:
            use_test_tables: Whether to use test_ prefix for local tables
            skip_indexes: Drop indexes before migration, recreate after (MUCH faster)
            method: 'bulk' (execute_values) or 'copy' (PostgreSQL COPY - fastest)
        """
        self.use_test_tables = use_test_tables
        self.skip_indexes = skip_indexes
        self.method = method
        self.local_conn = None
        self.rds_conn = None
        
        self.stats = {
            'total_records': 0,
            'migrated_records': 0,
            'failed_records': 0,
            'tables_migrated': 0
        }
        
        # Store dropped indexes for recreation
        self.dropped_indexes = {}
    
    def connect_local(self) -> psycopg2.extensions.connection:
        """Connect to local Docker PostgreSQL"""
        try:
            logger.info("Connecting to local PostgreSQL...")
            conn = psycopg2.connect(
                host='localhost',
                port=5434,
                database=os.environ['POSTGRES_DB'],
                user=os.environ['POSTGRES_USER'],
                password=os.environ['POSTGRES_PASSWORD']
            )
            conn.autocommit = False  # Manual commit for performance
            logger.info("✓ Connected to local PostgreSQL")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to local PostgreSQL: {str(e)}")
            raise
    
    def connect_rds(self) -> psycopg2.extensions.connection:
        """Connect to AWS RDS PostgreSQL"""
        try:
            if os.environ.get('RDS_HOST'):
                logger.info("Using RDS credentials from environment variables...")
                conn = psycopg2.connect(
                    host=os.environ['RDS_HOST'],
                    port=os.environ.get('RDS_PORT', 5432),
                    database=os.environ.get('RDS_DATABASE', 'condvest'),
                    user=os.environ['RDS_USER'],
                    password=os.environ['RDS_PASSWORD'],
                    sslmode='require'
                )
                conn.autocommit = False  # Manual commit for performance
                logger.info("✓ Connected to AWS RDS PostgreSQL")
                return conn
            else:
                raise Exception("RDS credentials not found in environment")
                
        except Exception as e:
            logger.error(f"Failed to connect to AWS RDS: {str(e)}")
            raise
    
    def drop_indexes(self, table_name: str):
        """Drop all indexes on table (except primary key) for faster insertion"""
        if not self.skip_indexes:
            return
        
        try:
            with self.rds_conn.cursor() as cursor:
                # Get all indexes except primary key
                cursor.execute(f"""
                    SELECT indexname, indexdef 
                    FROM pg_indexes 
                    WHERE tablename = %s 
                    AND schemaname = 'public'
                    AND indexname NOT LIKE '%%_pkey'
                """, (table_name,))
                
                indexes = cursor.fetchall()
                self.dropped_indexes[table_name] = indexes
                
                # Drop each index
                for index_name, index_def in indexes:
                    cursor.execute(f"DROP INDEX IF EXISTS {index_name}")
                    logger.info(f"  Dropped index: {index_name}")
                
                self.rds_conn.commit()
                logger.info(f"Dropped {len(indexes)} indexes on {table_name}")
                
        except Exception as e:
            logger.error(f"Error dropping indexes: {str(e)}")
            self.rds_conn.rollback()
    
    def recreate_indexes(self, table_name: str):
        """Recreate indexes after migration"""
        if not self.skip_indexes or table_name not in self.dropped_indexes:
            return
        
        try:
            with self.rds_conn.cursor() as cursor:
                for index_name, index_def in self.dropped_indexes[table_name]:
                    cursor.execute(index_def)
                    logger.info(f"  Recreated index: {index_name}")
                
                self.rds_conn.commit()
                logger.info(f"Recreated {len(self.dropped_indexes[table_name])} indexes on {table_name}")
                
        except Exception as e:
            logger.error(f"Error recreating indexes: {str(e)}")
            self.rds_conn.rollback()
    
    def migrate_table_copy(self, table_name: str) -> Tuple[int, int]:
        """
        Migrate table using PostgreSQL COPY (FASTEST method)
        10-100x faster than INSERT for large datasets
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"Migrating table: {table_name} (COPY method)")
        logger.info(f"{'='*80}")
        
        local_table_name = f"test_{table_name}" if self.use_test_tables else table_name
        
        try:
            # Step 1: Export from local to CSV buffer
            logger.info("Exporting from local database...")
            with self.local_conn.cursor() as local_cursor:
                # Get column names
                local_cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    ORDER BY ordinal_position
                """, (local_table_name,))
                columns = [row[0] for row in local_cursor.fetchall()]
                column_list = ', '.join(columns)
                
                # Create StringIO buffer for CSV
                buffer = io.StringIO()
                
                # Export to CSV
                copy_sql = f"COPY (SELECT {column_list} FROM {local_table_name}) TO STDOUT WITH CSV"
                local_cursor.copy_expert(copy_sql, buffer)
                
                # Get size of exported data
                buffer.seek(0, io.SEEK_END)
                export_size = buffer.tell()
                buffer.seek(0)
                
                logger.info(f"Exported {export_size:,} bytes to buffer")
            
            # Step 2: Drop indexes if requested
            if self.skip_indexes:
                self.drop_indexes(table_name)
            
            # Step 3: Import to RDS using COPY
            logger.info("Importing to RDS...")
            with self.rds_conn.cursor() as rds_cursor:
                # Use COPY FROM for super-fast bulk insert
                copy_sql = f"COPY {table_name} ({column_list}) FROM STDIN WITH CSV"
                rds_cursor.copy_expert(copy_sql, buffer)
                self.rds_conn.commit()
                
                # Get count
                rds_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = rds_cursor.fetchone()[0]
                
                logger.info(f"✓ Imported {count:,} records using COPY")
            
            # Step 4: Recreate indexes
            if self.skip_indexes:
                logger.info("Recreating indexes...")
                self.recreate_indexes(table_name)
            
            return count, 0
            
        except Exception as e:
            logger.error(f"Error in COPY migration: {str(e)}")
            self.rds_conn.rollback()
            raise
    
    def migrate_table_bulk(self, table_name: str, batch_size: int = 10000) -> Tuple[int, int]:
        """
        Migrate table using bulk INSERT (faster than default, slower than COPY)
        Good balance of speed and conflict handling
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"Migrating table: {table_name} (BULK method, batch={batch_size:,})")
        logger.info(f"{'='*80}")
        
        local_table_name = f"test_{table_name}" if self.use_test_tables else table_name
        
        try:
            # Drop indexes for faster insertion
            if self.skip_indexes:
                self.drop_indexes(table_name)
            
            # Get columns
            with self.local_conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    ORDER BY ordinal_position
                """, (local_table_name,))
                columns = [row[0] for row in cursor.fetchall()]
                column_list = ', '.join(columns)
            
            # Prepare INSERT statement (no conflict handling for speed)
            insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES %s"
            
            # Fetch and insert in large batches
            migrated = 0
            failed = 0
            
            with self.local_conn.cursor(name=f'fast_migrate_{table_name}') as local_cursor:
                local_cursor.itersize = batch_size
                local_cursor.execute(f"SELECT {column_list} FROM {local_table_name}")
                
                batch = []
                for row in local_cursor:
                    batch.append(row)
                    
                    if len(batch) >= batch_size:
                        try:
                            with self.rds_conn.cursor() as rds_cursor:
                                psycopg2.extras.execute_values(
                                    rds_cursor,
                                    insert_sql,
                                    batch,
                                    page_size=batch_size
                                )
                                self.rds_conn.commit()
                                migrated += len(batch)
                                logger.info(f"  ✓ Migrated {migrated:,} records...")
                        except Exception as e:
                            logger.error(f"  ✗ Batch failed: {str(e)}")
                            failed += len(batch)
                            self.rds_conn.rollback()
                        
                        batch = []
                
                # Process remaining
                if batch:
                    try:
                        with self.rds_conn.cursor() as rds_cursor:
                            psycopg2.extras.execute_values(
                                rds_cursor,
                                insert_sql,
                                batch,
                                page_size=len(batch)
                            )
                            self.rds_conn.commit()
                            migrated += len(batch)
                    except Exception as e:
                        logger.error(f"  ✗ Final batch failed: {str(e)}")
                        failed += len(batch)
                        self.rds_conn.rollback()
            
            logger.info(f"✓ Migrated {migrated:,} records")
            
            # Recreate indexes
            if self.skip_indexes:
                logger.info("Recreating indexes...")
                self.recreate_indexes(table_name)
            
            return migrated, failed
            
        except Exception as e:
            logger.error(f"Error migrating table: {str(e)}")
            self.rds_conn.rollback()
            raise
    
    def migrate_all(self, tables: Optional[List[str]] = None) -> Dict:
        """Migrate all tables using fast method"""
        try:
            self.local_conn = self.connect_local()
            self.rds_conn = self.connect_rds()
            
            tables_to_migrate = tables if tables else self.TABLES
            
            logger.info(f"\nMigrating {len(tables_to_migrate)} tables using {self.method.upper()} method")
            logger.info(f"Skip indexes: {self.skip_indexes}")
            logger.info(f"Use test tables: {self.use_test_tables}")
            
            start_time = datetime.now()
            
            for table in tables_to_migrate:
                try:
                    if self.method == 'copy':
                        migrated, failed = self.migrate_table_copy(table)
                    else:
                        migrated, failed = self.migrate_table_bulk(table, batch_size=10000)
                    
                    self.stats['migrated_records'] += migrated
                    self.stats['failed_records'] += failed
                    self.stats['tables_migrated'] += 1
                    
                except Exception as e:
                    logger.error(f"Failed to migrate {table}: {str(e)}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Summary
            logger.info(f"\n{'='*80}")
            logger.info("MIGRATION SUMMARY")
            logger.info(f"{'='*80}")
            logger.info(f"Tables migrated: {self.stats['tables_migrated']}/{len(tables_to_migrate)}")
            logger.info(f"Records migrated: {self.stats['migrated_records']:,}")
            logger.info(f"Records failed: {self.stats['failed_records']:,}")
            logger.info(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
            logger.info(f"Rate: {self.stats['migrated_records']/duration:.0f} records/second")
            logger.info(f"{'='*80}\n")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Migration failed: {str(e)}")
            raise
        finally:
            if self.local_conn:
                self.local_conn.close()
            if self.rds_conn:
                self.rds_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Fast PostgreSQL migration for large datasets',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--use-test-tables', action='store_true',
                       help='Use test_ prefix for local database tables')
    parser.add_argument('--skip-indexes', action='store_true',
                       help='Drop indexes before migration, recreate after (MUCH faster)')
    parser.add_argument('--method', type=str, choices=['bulk', 'copy'], default='bulk',
                       help='Migration method: bulk (INSERT) or copy (COPY - fastest)')
    parser.add_argument('--table', type=str,
                       help='Migrate only specific table')
    
    args = parser.parse_args()
    
    # Banner
    print(f"""
{'='*80}
FAST PostgreSQL Database Migration: Local Docker → AWS RDS
{'='*80}
Method:          {args.method.upper()} {'(FASTEST!)' if args.method == 'copy' else '(FAST)'}
Skip indexes:    {args.skip_indexes} {'(Recommended for large datasets)' if args.skip_indexes else ''}
Use test tables: {args.use_test_tables}
Batch size:      10,000 records (optimized for speed)
{'='*80}
""")
    
    confirm = input("⚠️  This will migrate data to production RDS. Continue? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Migration cancelled.")
        return
    
    # Run migration
    migrator = FastDatabaseMigration(
        use_test_tables=args.use_test_tables,
        skip_indexes=args.skip_indexes,
        method=args.method
    )
    
    tables = [args.table] if args.table else None
    
    try:
        migrator.migrate_all(tables=tables)
        logger.info("✓ Migration completed successfully!")
    except Exception as e:
        logger.error(f"✗ Migration failed: {str(e)}")
        exit(1)


if __name__ == '__main__':
    main()
