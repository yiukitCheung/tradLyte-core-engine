"""
RDS PostgreSQL + TimescaleDB client for Speed Layer
Simplified version without batch layer dependencies
"""

import boto3
import json
import logging
import psycopg2
import psycopg2.extras
from typing import Dict, Any, List
import os

logger = logging.getLogger(__name__)


class RDSTimescaleClient:
    """Client for RDS PostgreSQL + TimescaleDB database operations (Speed Layer)"""
    
    def __init__(self, endpoint: str = None, port: str = None, 
                 username: str = None, password: str = None, 
                 database: str = None, secret_arn: str = None):
        """
        Initialize RDS TimescaleDB client
        
        Can use either direct credentials or AWS Secrets Manager
        """
        self.secret_arn = secret_arn
        
        if secret_arn:
            # Use AWS Secrets Manager
            self._load_credentials_from_secrets()
        else:
            # Use direct credentials
            self.endpoint = endpoint or os.environ.get('RDS_ENDPOINT')
            self.port = port or os.environ.get('RDS_PORT', '5432')
            self.username = username or os.environ.get('RDS_USERNAME')
            self.password = password or os.environ.get('RDS_PASSWORD')
            self.database = database or os.environ.get('RDS_DATABASE')
        
        self.connection = None
        self._connect()
    
    def _load_credentials_from_secrets(self):
        """Load database credentials from AWS Secrets Manager"""
        try:
            secrets_client = boto3.client('secretsmanager')
            response = secrets_client.get_secret_value(SecretId=self.secret_arn)
            secret = json.loads(response['SecretString'])
            
            self.endpoint = secret['host']
            self.port = secret.get('port', '5432')
            self.username = secret['username']
            self.password = secret['password']
            self.database = secret.get('dbname', secret.get('database'))
            
            logger.info("Loaded RDS credentials from Secrets Manager")
            
        except Exception as e:
            logger.error(f"Error loading credentials from Secrets Manager: {str(e)}")
            raise
    
    def _connect(self):
        """Establish connection to RDS PostgreSQL + TimescaleDB"""
        try:
            # Determine SSL mode: disable for local connections, require for RDS
            sslmode = os.environ.get('RDS_SSLMODE', 'require')
            # Auto-detect local connections (localhost, 127.0.0.1, container names, or host.docker.internal)
            if self.endpoint in ['localhost', '127.0.0.1', 'timescaledb', 'host.docker.internal'] or 'local' in self.endpoint.lower():
                sslmode = 'disable'
            
            self.connection = psycopg2.connect(
                host=self.endpoint,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                sslmode=sslmode
            )
            self.connection.autocommit = True
            
            # Verify TimescaleDB extension (optional - service can work without it)
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT extname FROM pg_extension WHERE extname = 'timescaledb';")
                    result = cursor.fetchone()
                    if not result:
                        logger.warning("TimescaleDB extension not found - attempting to create it")
                        try:
                            cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
                            logger.info("TimescaleDB extension created successfully")
                        except Exception as ext_error:
                            logger.warning(f"TimescaleDB extension not available (this is OK for regular PostgreSQL): {str(ext_error)}")
                            logger.info("Continuing with regular PostgreSQL (TimescaleDB features will be unavailable)")
                
                logger.info("Connected to RDS PostgreSQL + TimescaleDB")
            except Exception as ext_check_error:
                logger.warning(f"Could not verify TimescaleDB extension (continuing anyway): {str(ext_check_error)}")
                logger.info("Connected to RDS PostgreSQL (TimescaleDB status unknown)")
            
        except Exception as e:
            logger.error(f"Error connecting to RDS TimescaleDB: {str(e)}")
            raise
    
    def execute_query(self, sql: str, parameters: tuple = None) -> List[Dict[str, Any]]:
        """Execute a SQL query with optional parameters"""
        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(sql, parameters)
                
                # Return results for SELECT queries
                if cursor.description:
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
                else:
                    # For INSERT/UPDATE/DELETE, return affected rows
                    return [{'affected_rows': cursor.rowcount}]
                    
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"SQL: {sql}")
            logger.error(f"Parameters: {parameters}")
            raise
    
    def get_active_symbols(self) -> List[str]:
        """Get list of active symbols from symbol_metadata table
        
        Returns empty list if table doesn't exist (e.g., local testing without schema)
        """
        sql = """
            SELECT symbol 
            FROM symbol_metadata 
            WHERE active = 'true'
            ORDER BY symbol
        """
        
        try:
            results = self.execute_query(sql)
            symbols = [row['symbol'] for row in results]
            
            logger.info(f"Retrieved {len(symbols)} active symbols")
            return symbols
            
        except psycopg2.errors.UndefinedTable as e:
            # Table doesn't exist - expected in local testing
            logger.warning(f"symbol_metadata table not found (expected in local testing): {str(e)}")
            return []
        except Exception as e:
            # Other database errors - log as error
            logger.error(f"Error getting active symbols: {str(e)}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("RDS TimescaleDB connection closed")
