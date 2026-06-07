-- ============================================================================
-- TRADLYTE DATA PIPELINE - PostgreSQL Schema Initialization
-- ============================================================================
-- Database: AWS RDS PostgreSQL (Standard, without TimescaleDB)
-- Purpose: Complete schema for Lambda-based batch data pipeline
-- Version: 2.0 (with watermark table and job tracking)
-- Date: 2025-11-16
-- ============================================================================
-- Architecture:
--   Bronze Layer: raw_ohlcv (source of truth, 5-year retention in RDS)
--   Silver Layer: silver_3d, 5d, 8d, 13d, 21d, 34d (Fibonacci resampled)
--   Metadata: symbol_metadata (stock/asset information)
--   Operations: batch_jobs, data_ingestion_watermark (job tracking & SCD Type 2)
-- ============================================================================


-- ============================================================================
-- SECTION 1: METADATA LAYER
-- ============================================================================
-- Symbol metadata table - stores information about each tradable asset
-- ============================================================================

CREATE TABLE IF NOT EXISTS symbol_metadata (
    symbol VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    market VARCHAR(100),
    locale VARCHAR(100),
    active VARCHAR(100),
    primary_exchange VARCHAR(100),
    type VARCHAR(100),
    marketCap BIGINT,
    industry VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE symbol_metadata IS 'Metadata for all tradable symbols (stocks, ETFs, crypto, forex)';
COMMENT ON COLUMN symbol_metadata.symbol IS 'Ticker symbol (e.g., AAPL, BTC-USD)';
COMMENT ON COLUMN symbol_metadata.active IS 'Whether symbol is actively traded';
COMMENT ON COLUMN symbol_metadata.type IS 'Asset type: CS (common stock), ETF, CRYPTO, FX';


-- ============================================================================
-- SECTION 2: BRONZE LAYER (SOURCE OF TRUTH)
-- ============================================================================
-- Raw OHLCV data - daily price candles
-- Retention: Last 5 years in RDS, all history in S3 data lake
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw_ohlcv (
    symbol VARCHAR(50) NOT NULL,
    open DECIMAL(10,2) NOT NULL,
    high DECIMAL(10,2) NOT NULL,
    low DECIMAL(10,2) NOT NULL,
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    interval VARCHAR(10) NOT NULL DEFAULT '1d',
    PRIMARY KEY (symbol, timestamp, interval)
);

COMMENT ON TABLE raw_ohlcv IS 'Bronze layer: Raw OHLCV data (5-year retention in RDS, full history in S3)';
COMMENT ON COLUMN raw_ohlcv.interval IS 'Time interval: 1d (daily), 1m (minute), etc.';


-- ============================================================================
-- SECTION 3: SILVER LAYER (FIBONACCI RESAMPLED DATA)
-- ============================================================================
-- Fibonacci sequence resampling for multi-timeframe analysis
-- Intervals: 3, 5, 8, 13, 21, 34 days
-- ============================================================================

-- 3-Day Fibonacci interval
CREATE TABLE IF NOT EXISTS silver_3d (
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,2) NOT NULL,
    high DECIMAL(10,2) NOT NULL,
    low DECIMAL(10,2) NOT NULL,
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

-- 5-Day Fibonacci interval
CREATE TABLE IF NOT EXISTS silver_5d (
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,2) NOT NULL,
    high DECIMAL(10,2) NOT NULL,
    low DECIMAL(10,2) NOT NULL,
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

-- 8-Day Fibonacci interval
CREATE TABLE IF NOT EXISTS silver_8d (
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,2) NOT NULL,
    high DECIMAL(10,2) NOT NULL,
    low DECIMAL(10,2) NOT NULL,
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

-- 13-Day Fibonacci interval
CREATE TABLE IF NOT EXISTS silver_13d (
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,2) NOT NULL,
    high DECIMAL(10,2) NOT NULL,
    low DECIMAL(10,2) NOT NULL,
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

-- 21-Day Fibonacci interval
CREATE TABLE IF NOT EXISTS silver_21d (
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,2) NOT NULL,
    high DECIMAL(10,2) NOT NULL,
    low DECIMAL(10,2) NOT NULL,
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

-- 34-Day Fibonacci interval
CREATE TABLE IF NOT EXISTS silver_34d (
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10,2) NOT NULL,
    high DECIMAL(10,2) NOT NULL,
    low DECIMAL(10,2) NOT NULL,
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

COMMENT ON TABLE silver_3d IS 'Silver layer: 3-day Fibonacci resampled OHLCV data';
COMMENT ON TABLE silver_5d IS 'Silver layer: 5-day Fibonacci resampled OHLCV data';
COMMENT ON TABLE silver_8d IS 'Silver layer: 8-day Fibonacci resampled OHLCV data';
COMMENT ON TABLE silver_13d IS 'Silver layer: 13-day Fibonacci resampled OHLCV data';
COMMENT ON TABLE silver_21d IS 'Silver layer: 21-day Fibonacci resampled OHLCV data';
COMMENT ON TABLE silver_34d IS 'Silver layer: 34-day Fibonacci resampled OHLCV data';


-- ============================================================================
-- SECTION 4: OPERATIONAL METADATA TABLES
-- ============================================================================
-- Tables for tracking Lambda jobs and data ingestion progress
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 4A: Batch Jobs Table (Job Execution Tracking)
-- ---------------------------------------------------------------------------
-- Tracks execution of Lambda batch jobs for monitoring and debugging
-- Used by: daily_meta_fetcher, optional job metadata writers, and other Lambda functions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS batch_jobs (
    job_id VARCHAR(100) PRIMARY KEY,
    job_type VARCHAR(50) NOT NULL,      -- 'DAILY_OHLCV', 'WEEKLY_META', 'RESAMPLING', etc.
    status VARCHAR(20) NOT NULL,         -- 'RUNNING', 'COMPLETED', 'FAILED'
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    symbols_processed TEXT,              -- JSON array of symbols processed
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE batch_jobs IS 'Tracks batch processing jobs executed by Lambda functions';
COMMENT ON COLUMN batch_jobs.job_id IS 'Unique identifier (e.g., daily-ohlcv-backfill-1234567890)';
COMMENT ON COLUMN batch_jobs.job_type IS 'Type of job: DAILY_OHLCV, WEEKLY_META, RESAMPLING, etc.';
COMMENT ON COLUMN batch_jobs.status IS 'Job status: RUNNING, COMPLETED, FAILED';
COMMENT ON COLUMN batch_jobs.symbols_processed IS 'JSON array of symbols processed in this job';
COMMENT ON COLUMN batch_jobs.records_processed IS 'Number of records successfully processed';

-- ---------------------------------------------------------------------------
-- 4B: Data Ingestion Watermark Table (SCD Type 2)
-- ---------------------------------------------------------------------------
-- Industry-standard watermark pattern for incremental data loading
-- Tracks latest ingested date per symbol with full audit history
-- Enables fast missing date detection without scanning 480K+ S3 files
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS data_ingestion_watermark (
    watermark_id BIGSERIAL PRIMARY KEY,     -- Surrogate key (auto-increment)
    symbol VARCHAR(20) NOT NULL,             -- Stock symbol (not unique, supports history)
    latest_date DATE NOT NULL,               -- Latest date ingested in this update
    ingested_at TIMESTAMP DEFAULT NOW(),     -- When this record was created
    records_count BIGINT DEFAULT 0,          -- Records processed in this update
    is_current BOOLEAN DEFAULT TRUE          -- TRUE = current watermark (only one per symbol)
);

COMMENT ON TABLE data_ingestion_watermark IS 'SCD Type 2: Tracks ingestion progress with full audit history';
COMMENT ON COLUMN data_ingestion_watermark.watermark_id IS 'Surrogate key (auto-increment)';
COMMENT ON COLUMN data_ingestion_watermark.symbol IS 'Stock symbol (e.g., AAPL) - allows multiple rows for history';
COMMENT ON COLUMN data_ingestion_watermark.latest_date IS 'Latest date successfully ingested';
COMMENT ON COLUMN data_ingestion_watermark.ingested_at IS 'Timestamp when this record was created';
COMMENT ON COLUMN data_ingestion_watermark.records_count IS 'Records processed in this specific update';
COMMENT ON COLUMN data_ingestion_watermark.is_current IS 'TRUE if current watermark (enforced: only 1 per symbol)';

-- ============================================================================
-- SECTION 5: SCANNER LAYER
-- ============================================================================
-- Staging + final picks. Staging DDL is canonical in
-- shared/database/sql/daily_scan_signals.sql (keep in sync).
-- ============================================================================

CREATE TABLE IF NOT EXISTS daily_scan_signals (
    scan_date     DATE         NOT NULL,
    worker_idx    SMALLINT     NOT NULL,
    symbol        VARCHAR(50)  NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    signal        VARCHAR(10)  NOT NULL CHECK (signal IN ('BUY', 'SELL', 'HOLD')),
    price         DECIMAL(12,4) NOT NULL,
    confidence    DECIMAL(5,4),
    metadata      JSONB        DEFAULT '{}'::jsonb,
    created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scan_date, symbol, strategy_name)
);

COMMENT ON TABLE daily_scan_signals IS 'Scanner staging table; aggregator clears rows after stock_picks are written.';

CREATE INDEX IF NOT EXISTS idx_daily_scan_signals_date
ON daily_scan_signals(scan_date);

CREATE TABLE IF NOT EXISTS stock_picks (
    scan_date DATE NOT NULL,
    rank INTEGER NOT NULL CHECK (rank > 0),
    symbol VARCHAR(50) NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    signal VARCHAR(10) NOT NULL CHECK (signal IN ('BUY', 'SELL', 'HOLD')),
    price DECIMAL(12,4) NOT NULL,
    confidence DECIMAL(5,4) NOT NULL DEFAULT 0.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    score DECIMAL(8,6) NOT NULL DEFAULT 0.0,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scan_date, rank),
    UNIQUE (scan_date, symbol, strategy_name)
);

COMMENT ON TABLE stock_picks IS 'Ranked top picks produced by the daily scanner run';

CREATE INDEX IF NOT EXISTS idx_stock_picks_date_rank
ON stock_picks(scan_date, rank);

CREATE INDEX IF NOT EXISTS idx_stock_picks_symbol_date
ON stock_picks(symbol, scan_date);

-- ============================================================================
-- SECTION 6: PERFORMANCE INDEXES
-- ============================================================================
-- Indexes optimized for time-series queries and Lambda operations
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 6A: Bronze Layer Indexes (raw_ohlcv)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_raw_ohlcv_symbol_timestamp ON raw_ohlcv(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_ohlcv_timestamp ON raw_ohlcv(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_ohlcv_interval ON raw_ohlcv(interval);
CREATE INDEX IF NOT EXISTS idx_raw_ohlcv_symbol ON raw_ohlcv(symbol);

-- ---------------------------------------------------------------------------
-- 6B: Silver Layer Indexes (Fibonacci resampled tables)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_silver_3d_symbol_date ON silver_3d(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_silver_3d_date ON silver_3d(date DESC);

CREATE INDEX IF NOT EXISTS idx_silver_5d_symbol_date ON silver_5d(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_silver_5d_date ON silver_5d(date DESC);

CREATE INDEX IF NOT EXISTS idx_silver_8d_symbol_date ON silver_8d(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_silver_8d_date ON silver_8d(date DESC);

CREATE INDEX IF NOT EXISTS idx_silver_13d_symbol_date ON silver_13d(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_silver_13d_date ON silver_13d(date DESC);

CREATE INDEX IF NOT EXISTS idx_silver_21d_symbol_date ON silver_21d(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_silver_21d_date ON silver_21d(date DESC);

CREATE INDEX IF NOT EXISTS idx_silver_34d_symbol_date ON silver_34d(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_silver_34d_date ON silver_34d(date DESC);

-- ---------------------------------------------------------------------------
-- 6C: Metadata Layer Indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_symbol_metadata_active ON symbol_metadata(active);
CREATE INDEX IF NOT EXISTS idx_symbol_metadata_type ON symbol_metadata(type);
CREATE INDEX IF NOT EXISTS idx_symbol_metadata_market ON symbol_metadata(market);

-- ---------------------------------------------------------------------------
-- 6D: Operational Metadata Indexes
-- ---------------------------------------------------------------------------
-- Batch Jobs indexes
CREATE INDEX IF NOT EXISTS idx_batch_jobs_job_type ON batch_jobs(job_type);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_status ON batch_jobs(status);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_start_time ON batch_jobs(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_created_at ON batch_jobs(created_at DESC);

-- Data Ingestion Watermark indexes (optimized for SCD Type 2 queries)
-- Partial unique index: Ensures only ONE current record per symbol
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_current_symbol 
ON data_ingestion_watermark(symbol) 
WHERE is_current = TRUE;

-- Fast lookup of current watermarks
CREATE INDEX IF NOT EXISTS idx_watermark_symbol_current 
ON data_ingestion_watermark(symbol, is_current) 
WHERE is_current = TRUE;

-- Find oldest ingested symbols
CREATE INDEX IF NOT EXISTS idx_watermark_latest_date 
ON data_ingestion_watermark(latest_date) 
WHERE is_current = TRUE;


-- ============================================================================
-- SECTION 7: TABLE STATISTICS UPDATE
-- ============================================================================
-- Update PostgreSQL query planner statistics for optimal query performance
-- ============================================================================

ANALYZE symbol_metadata;
ANALYZE raw_ohlcv;
ANALYZE silver_3d;
ANALYZE silver_5d;
ANALYZE silver_8d;
ANALYZE silver_13d;
ANALYZE silver_21d;
ANALYZE silver_34d;
ANALYZE batch_jobs;
ANALYZE data_ingestion_watermark;


-- ============================================================================
-- SECTION 8: VERIFICATION QUERIES
-- ============================================================================
-- Run these queries to verify successful schema creation
-- ============================================================================

-- Check all tables exist
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'public' 
AND tablename IN (
    'symbol_metadata', 
    'raw_ohlcv', 
    'silver_3d', 'silver_5d', 'silver_8d', 'silver_13d', 'silver_21d', 'silver_34d',
    'batch_jobs',
    'data_ingestion_watermark'
)
ORDER BY tablename;

-- Check indexes
SELECT 
    indexname,
    tablename
FROM pg_indexes 
WHERE schemaname = 'public' 
AND tablename IN (
    'symbol_metadata', 
    'raw_ohlcv', 
    'silver_3d', 'silver_5d', 'silver_8d', 'silver_13d', 'silver_21d', 'silver_34d',
    'batch_jobs',
    'data_ingestion_watermark'
)
ORDER BY tablename, indexname;

-- Check table sizes
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public' 
AND tablename IN (
    'symbol_metadata', 
    'raw_ohlcv', 
    'silver_3d', 'silver_5d', 'silver_8d', 'silver_13d', 'silver_21d', 'silver_34d',
    'batch_jobs',
    'data_ingestion_watermark'
)
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check watermark table structure (SCD Type 2 validation)
SELECT 
    watermark_id,
    symbol,
    latest_date,
    ingested_at,
    is_current
FROM data_ingestion_watermark
WHERE symbol = 'AAPL'
ORDER BY ingested_at DESC
LIMIT 5;


-- ============================================================================
-- SECTION 9: SUCCESS MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '==============================================================================';
    RAISE NOTICE 'TRADLYTE DATA PIPELINE - Schema Initialization Complete!';
    RAISE NOTICE '==============================================================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Tables Created:';
    RAISE NOTICE '  ✅ Metadata Layer:';
    RAISE NOTICE '     - symbol_metadata (asset metadata)';
    RAISE NOTICE '';
    RAISE NOTICE '  ✅ Bronze Layer (Source of Truth):';
    RAISE NOTICE '     - raw_ohlcv (daily OHLCV, 5-year retention)';
    RAISE NOTICE '';
    RAISE NOTICE '  ✅ Silver Layer (Fibonacci Resampled):';
    RAISE NOTICE '     - silver_3d, silver_5d, silver_8d, silver_13d, silver_21d, silver_34d';
    RAISE NOTICE '';
    RAISE NOTICE '  ✅ Operational Metadata:';
    RAISE NOTICE '     - batch_jobs (Lambda job tracking)';
    RAISE NOTICE '     - data_ingestion_watermark (SCD Type 2 ingestion tracking)';
    RAISE NOTICE '';
    RAISE NOTICE 'Indexes: All performance indexes created for time-series queries';
    RAISE NOTICE 'Ready for: Lambda-based data ingestion and Fibonacci resampling';
    RAISE NOTICE '';
    RAISE NOTICE '==============================================================================';
END $$;
