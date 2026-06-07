-- Kinesis Analytics Flink SQL: 1-Minute OHLCV Resampling
-- Purpose: Process raw 1-minute data from WebSocket and standardize format
-- Input: Raw Polygon WebSocket data from Kinesis Stream
-- Output: Standardized 1-minute OHLCV data

-- Create input stream from Kinesis Data Stream
CREATE TABLE polygon_raw_stream (
    record_type VARCHAR(20),
    symbol VARCHAR(10),
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT,
    timestamp_str VARCHAR(30),
    interval_type VARCHAR(5),
    source VARCHAR(50),
    ingestion_time VARCHAR(30),
    WATERMARK FOR ROWTIME() AS ROWTIME() - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kinesis',
    'stream' = 'market-data-raw',
    'aws.region' = 'ca-west-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json'
);

-- Create output stream for processed 1-minute OHLCV
CREATE TABLE ohlcv_1min_stream (
    symbol VARCHAR(10),
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    volume BIGINT,
    trade_count INT,
    vwap DOUBLE,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    interval_type VARCHAR(5),
    processing_time TIMESTAMP(3)
) WITH (
    'connector' = 'kinesis',
    'stream' = 'market-data-1min',
    'aws.region' = 'ca-west-1',
    'format' = 'json'
);

-- Process and standardize 1-minute OHLCV data
INSERT INTO ohlcv_1min_stream
SELECT 
    symbol,
    FIRST_VALUE(open_price) AS open_price,
    MAX(high_price) AS high_price,
    MIN(low_price) AS low_price,
    LAST_VALUE(close_price) AS close_price,
    SUM(volume) AS volume,
    COUNT(*) AS trade_count,
    
    -- Calculate VWAP (Volume Weighted Average Price)
    CASE 
        WHEN SUM(volume) > 0 THEN 
            SUM(close_price * volume) / SUM(volume)
        ELSE 
            AVG(close_price)
    END AS vwap,
    
    -- Window timing
    TUMBLE_START(ROWTIME(), INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(ROWTIME(), INTERVAL '1' MINUTE) AS window_end,
    '1m' AS interval_type,
    CURRENT_TIMESTAMP AS processing_time

FROM polygon_raw_stream
WHERE 
    record_type = 'ohlcv'
    AND symbol IS NOT NULL
    AND open_price > 0 
    AND high_price > 0 
    AND low_price > 0 
    AND close_price > 0
    AND volume >= 0
GROUP BY 
    symbol,
    TUMBLE(ROWTIME(), INTERVAL '1' MINUTE);
