-- Kinesis Analytics Flink SQL: 2-Hour OHLCV Resampling
-- Purpose: Aggregate 1-hour data into 2-hour candles
-- Input: 1-hour OHLCV data from 1hour_resampler
-- Output: 2-hour OHLCV candles

-- Create input stream from 1-hour processed data
CREATE TABLE ohlcv_1hour_input (
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
    WATERMARK FOR window_end AS window_end - INTERVAL '5' MINUTE
) WITH (
    'connector' = 'kinesis',
    'stream' = 'market-data-1hour',
    'aws.region' = 'ca-west-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json'
);

-- Create output stream for 2-hour OHLCV
CREATE TABLE ohlcv_2hour_stream (
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
    'stream' = 'market-data-2hour',
    'aws.region' = 'ca-west-1',
    'format' = 'json'
);

-- Aggregate 1-hour data into 2-hour candles
INSERT INTO ohlcv_2hour_stream
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
    
    -- Volume-weighted average price for 2 hours
    CASE 
        WHEN SUM(volume) > 0 THEN 
            SUM(vwap * volume) / SUM(volume)
        ELSE 
            AVG(close_price)
    END AS vwap,
    
    -- Window timing (2-hour windows)
    TUMBLE_START(window_end, INTERVAL '2' HOUR) AS window_start,
    TUMBLE_END(window_end, INTERVAL '2' HOUR) AS window_end,
    '2h' AS interval_type,
    CURRENT_TIMESTAMP AS processing_time

FROM ohlcv_1hour_input
WHERE 
    interval_type = '1h'
    AND symbol IS NOT NULL
    AND volume >= 0
GROUP BY 
    symbol,
    TUMBLE(window_end, INTERVAL '2' HOUR)
HAVING 
    COUNT(*) > 0;
