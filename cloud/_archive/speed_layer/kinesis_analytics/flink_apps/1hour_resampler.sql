-- Kinesis Analytics Flink SQL: 1-Hour OHLCV Resampling
-- Purpose: Aggregate 15-minute data into 1-hour candles
-- Input: 15-minute OHLCV data from 15min_resampler
-- Output: 1-hour OHLCV candles

-- Create input stream from 15-minute processed data
CREATE TABLE ohlcv_15min_input (
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
    processing_time TIMESTAMP(3),
    WATERMARK FOR window_end AS window_end - INTERVAL '2' MINUTE
) WITH (
    'connector' = 'kinesis',
    'stream' = 'market-data-15min',
    'aws.region' = 'ca-west-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json'
);

-- Create output stream for 1-hour OHLCV
CREATE TABLE ohlcv_1hour_stream (
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
    'stream' = 'market-data-1hour',
    'aws.region' = 'ca-west-1',
    'format' = 'json'
);

-- Aggregate 15-minute data into 1-hour candles
INSERT INTO ohlcv_1hour_stream
SELECT 
    symbol,
    
    -- OHLC aggregation
    FIRST_VALUE(open_price ORDER BY window_start ASC) AS open_price,
    MAX(high_price) AS high_price,
    MIN(low_price) AS low_price,
    LAST_VALUE(close_price ORDER BY window_start ASC) AS close_price,
    
    -- Volume and trade aggregation
    SUM(volume) AS volume,
    SUM(trade_count) AS trade_count,
    
    -- Volume-weighted average price for the hour
    CASE 
        WHEN SUM(volume) > 0 THEN 
            SUM(vwap * volume) / SUM(volume)
        ELSE 
            AVG(close_price)
    END AS vwap,
    
    -- Window timing (1-hour windows)
    TUMBLE_START(window_end, INTERVAL '1' HOUR) AS window_start,
    TUMBLE_END(window_end, INTERVAL '1' HOUR) AS window_end,
    '1h' AS interval_type,
    CURRENT_TIMESTAMP AS processing_time

FROM ohlcv_15min_input
WHERE 
    interval_type = '15m'
    AND symbol IS NOT NULL
    AND volume >= 0
GROUP BY 
    symbol,
    TUMBLE(window_end, INTERVAL '1' HOUR)
HAVING 
    COUNT(*) > 0;
