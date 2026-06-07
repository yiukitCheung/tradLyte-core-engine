"""
RDS PostgreSQL + TimescaleDB client for database operations
Cost-efficient alternative to Aurora for time-series data
"""

import boto3
import json
import logging
import psycopg2
import psycopg2.extras
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, date
from decimal import Decimal
import os

from ..models.data_models import OHLCVData

logger = logging.getLogger(__name__)


class RDSTimescaleClient:
    """Client for RDS PostgreSQL + TimescaleDB database operations"""

    @classmethod
    def from_lambda_environment(cls) -> "RDSTimescaleClient":
        """
        Build client from Lambda env. Prefer RDS_SECRET_ARN + Secrets Manager.

        If the function runs in a VPC private subnet without NAT or a Secrets Manager
        VPC endpoint, Secrets Manager HTTPS calls will time out. Then set
        RDS_USE_ENV_CREDENTIALS=true and RDS_ENDPOINT, RDS_USERNAME, RDS_PASSWORD, RDS_DATABASE.
        """
        use_env = os.environ.get("RDS_USE_ENV_CREDENTIALS", "").lower() in ("1", "true", "yes")
        secret_arn = (os.environ.get("RDS_SECRET_ARN") or "").strip()
        if use_env or not secret_arn:
            required = ("RDS_ENDPOINT", "RDS_USERNAME", "RDS_PASSWORD", "RDS_DATABASE")
            missing = [k for k in required if not os.environ.get(k)]
            if missing:
                raise ValueError(
                    "RDS_USE_ENV_CREDENTIALS or empty RDS_SECRET_ARN requires env: "
                    + ", ".join(missing)
                )
            logger.info("RDSTimescaleClient: env credentials (no Secrets Manager API)")
            return cls(secret_arn=None)
        logger.info("RDSTimescaleClient: Secrets Manager (%s)", secret_arn)
        return cls(secret_arn=secret_arn)

    def __init__(
        self,
        *,
        endpoint: str = None,
        port: str = None,
        username: str = None,
        password: str = None,
        database: str = None,
        secret_arn: str = None,
    ):
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
            # Optional: Interface VPC endpoint URL when private DNS is disabled, e.g.
            # https://vpce-....secretsmanager.ca-west-1.vpce.amazonaws.com
            endpoint_url = (os.environ.get("SECRETS_MANAGER_ENDPOINT_URL") or "").strip() or None
            if endpoint_url:
                logger.info("Secrets Manager boto client using SECRETS_MANAGER_ENDPOINT_URL")
            secrets_client = boto3.client(
                "secretsmanager",
                **({"endpoint_url": endpoint_url} if endpoint_url else {}),
            )
            response = secrets_client.get_secret_value(SecretId=self.secret_arn)
            secret = json.loads(response['SecretString'])
            
            self.endpoint = secret['host']
            self.port = secret.get('port', '5432')
            self.username = secret['username']
            self.password = secret['password']
            self.database = secret.get('dbname', secret.get('database'))
            
            logger.info("Loaded RDS credentials from Secrets Manager")
            
        except Exception as e:
            if type(e).__name__ == "ConnectTimeoutError" or "Connect timeout" in str(e).lower():
                logger.error("Secrets Manager connect timeout (VPC cannot reach API): %s", e)
                raise RuntimeError(
                    "Secrets Manager HTTPS timed out. Lambdas in a private VPC need either: "
                    "(1) NAT Gateway for outbound internet, or "
                    "(2) Interface VPC endpoint com.amazonaws.<region>.secretsmanager with SG allowing 443 from this Lambda, or "
                    "(3) set SECRETS_MANAGER_ENDPOINT_URL to your VPC endpoint URL if private DNS is off, or "
                    "(4) set RDS_USE_ENV_CREDENTIALS=true and RDS_ENDPOINT/RDS_USERNAME/RDS_PASSWORD/RDS_DATABASE. "
                    "See cloud/batch_layer/infrastructure/common/VPC_LAMBDA_SECRETS_MANAGER.txt"
                ) from e
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
    
    def get_active_symbols(
        self,
        types: Optional[List[str]] = None,
        min_market_cap: Optional[int] = None,
        max_market_cap: Optional[int] = None,
        industry_contains: Optional[Union[str, List[str]]] = None,
    ) -> List[str]:
        """Get list of active symbols from symbol_metadata with optional filters.

        Active means LOWER(TRIM(active)) = 'true'. All filters are optional.

        Args:
            types: Include only these asset types (e.g. ['CS', 'ETF', 'ETV', 'UNIT', 'PFD', 'ADRC']).
                   Polygon types: CS (common stock), ETF, ETV, UNIT, PFD, ADRC, etc.
            min_market_cap: Minimum market cap (inclusive). Column: marketcap.
            max_market_cap: Maximum market cap (inclusive). Column: marketcap.
            industry_contains: Match industry by substring (case-insensitive).
                              Single string: industry ILIKE '%term%'.
                              List of strings: symbol matches if industry contains any of the terms (OR).

        Returns:
            List of symbol tickers. Empty list if table doesn't exist (e.g. local testing).
        """
        conditions = ["LOWER(TRIM(active)) = 'true'"]
        parameters: List[Any] = []

        if types:
            conditions.append(f"type IN ({','.join('%s' for _ in types)})")
            parameters.extend(types)
        if min_market_cap is not None:
            conditions.append("marketcap >= %s")
            parameters.append(min_market_cap)
        if max_market_cap is not None:
            conditions.append("marketcap <= %s")
            parameters.append(max_market_cap)
        if industry_contains is not None:
            terms = [industry_contains] if isinstance(industry_contains, str) else industry_contains
            terms = [t for t in terms if t is not None and str(t).strip()]
            if terms:
                placeholders = " OR ".join(
                    "industry ILIKE %s" for _ in terms
                )
                conditions.append(f"({placeholders})")
                parameters.extend(f"%{str(t).strip()}%" for t in terms)

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT symbol
            FROM symbol_metadata
            WHERE {where_clause}
            ORDER BY symbol
        """
        try:
            results = self.execute_query(sql, tuple(parameters) if parameters else None)
            symbols = [row['symbol'] for row in results]
            logger.info(
                f"Retrieved {len(symbols)} active symbols"
                + (f" (types={types}, industry_contains={industry_contains})" if types or industry_contains else "")
            )
            return symbols
        except psycopg2.errors.UndefinedTable as e:
            logger.warning(f"symbol_metadata table not found (expected in local testing): {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error getting active symbols: {str(e)}")
            raise
    
    def insert_ohlcv_data(self, ohlcv_data: List[OHLCVData]) -> int:
        """Insert OHLCV data into the raw_ohlcv table (TimescaleDB optimized)"""
        if not ohlcv_data:
            return 0
        
        # Use TimescaleDB-optimized bulk insert
        sql = """
        INSERT INTO raw_ohlcv (timestamp, symbol, open, high, low, close, volume, interval)
        VALUES %s
        ON CONFLICT (timestamp, symbol, interval) 
        DO UPDATE SET 
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
        
        try:
            # Prepare data for bulk insert
            data_tuples = []
            for ohlcv in ohlcv_data:
                data_tuples.append((
                    ohlcv.timestamp,
                    ohlcv.symbol,
                    float(ohlcv.open),
                    float(ohlcv.high),
                    float(ohlcv.low),
                    float(ohlcv.close),
                    ohlcv.volume,
                    ohlcv.interval
                ))
            
            # Use psycopg2.extras.execute_values for high-performance bulk insert
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_values(
                    cursor, sql, data_tuples, template=None, page_size=1000
                )
                records_inserted = cursor.rowcount
            
            logger.info(f"Inserted {records_inserted} OHLCV records into TimescaleDB")
            return records_inserted
            
        except Exception as e:
            logger.error(f"Error inserting OHLCV data: {str(e)}")
            raise
    
    def insert_metadata_batch(self, metadata_list: List[Dict[str, Any]]) -> int:
        """Insert metadata batch into symbol_metadata table"""
        if not metadata_list:
            return 0
        
        sql = """
        INSERT INTO symbol_metadata (
            symbol, name, market, locale, active, 
            primary_exchange, type, marketcap, industry, description
        )
        VALUES %s
        ON CONFLICT (symbol)
        DO UPDATE SET
            name = EXCLUDED.name,
            market = EXCLUDED.market,
            locale = EXCLUDED.locale,
            active = EXCLUDED.active,
            primary_exchange = EXCLUDED.primary_exchange,
            type = EXCLUDED.type,
            marketcap = EXCLUDED.marketcap,
            industry = EXCLUDED.industry,
            description = EXCLUDED.description
        """
        
        try:
            # Prepare data for bulk insert
            data_tuples = []
            for meta in metadata_list:
                data_tuples.append((
                    meta.get('symbol'),
                    meta.get('name'),
                    meta.get('market'),
                    meta.get('locale'),
                    meta.get('active', 'true'),
                    meta.get('primary_exchange'),
                    meta.get('type'),
                    meta.get('marketCap', 0),
                    meta.get('industry'),
                    meta.get('description')
                ))
            
            # Use psycopg2.extras.execute_values for high-performance bulk insert
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_values(
                    cursor, sql, data_tuples, template=None, page_size=100
                )
                records_inserted = cursor.rowcount
            
            logger.info(f"Inserted {records_inserted} metadata records into TimescaleDB")
            return records_inserted
            
        except Exception as e:
            logger.error(f"Error inserting metadata: {str(e)}")
            raise
    
    def get_latest_data_date(self, symbol: str = None) -> Optional[date]:
        """Get the latest data date for a symbol or all symbols"""
        if symbol:
            sql = """
            SELECT MAX(timestamp) as latest_date 
            FROM raw_ohlcv 
            WHERE symbol = %s
            """
            parameters = (symbol,)
        else:
            sql = """
            SELECT MAX(timestamp) as latest_date 
            FROM raw_ohlcv
            """
            parameters = None
        
        try:
            results = self.execute_query(sql, parameters)
            
            if results and results[0]['latest_date']:
                return results[0]['latest_date'].date()
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest data date: {str(e)}")
            raise
    
    def get_symbol_data(self, symbol: str, start_date: date = None, end_date: date = None) -> List[Dict]:
        """Get symbol data for a date range"""
        sql = """
        SELECT timestamp, symbol, open, high, low, close, volume
        FROM raw_ohlcv
        WHERE symbol = %s
        """
        
        parameters = [symbol]
        
        if start_date:
            sql += " AND timestamp >= %s"
            parameters.append(start_date)
        
        if end_date:
            sql += " AND timestamp <= %s"
            parameters.append(end_date)
        
        sql += " ORDER BY timestamp ASC"
        
        try:
            results = self.execute_query(sql, tuple(parameters))
            
            # Convert to standard format
            data = []
            for row in results:
                data.append({
                    'timestamp': row['timestamp'].isoformat(),
                    'symbol': row['symbol'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                })
            
            return data
            
        except Exception as e:
            logger.error(f"Error getting symbol data for {symbol}: {str(e)}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("RDS TimescaleDB connection closed")

