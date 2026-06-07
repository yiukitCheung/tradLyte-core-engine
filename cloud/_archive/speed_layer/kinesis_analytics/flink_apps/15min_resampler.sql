-- Kinesis Analytics Flink SQL: 15-Minute OHLCV Resampling
-- Purpose: Aggregate 5-minute data into 15-minute candles
-- Input: 5-minute OHLCV data from 5min_resampler
-- Output: 15-minute OHLCV candles

-- Create input stream from 5-minute processed data
CREATE TABLE ohlcv_5min_input (
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
    WATERMARK FOR window_end AS window_end - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'kinesis',
    'stream' = 'market-data-5min',
    'aws.region' = 'ca-west-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json'
);

-- Create output stream for 15-minute OHLCV
CREATE TABLE ohlcv_15min_stream (
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
    'stream' = 'market-data-15min',
    'aws.region' = 'ca-west-1',
    'format' = 'json'
);

-- Aggregate 5-minute data into 15-minute candles
INSERT INTO ohlcv_15min_stream
SELECT 
    symbol,
    
    -- OHLC aggregation (first open, highest high, lowest low, last close)
    FIRST_VALUE(open_price ORDER BY window_start ASC) AS open_price,
    MAX(high_price) AS high_price,
    MIN(low_price) AS low_price,
    LAST_VALUE(close_price ORDER BY window_start ASC) AS close_price,
    
    -- Volume and trade aggregation
    SUM(volume) AS volume,
    SUM(trade_count) AS trade_count,
    
    -- Recalculate volume-weighted average price
    CASE 
        WHEN SUM(volume) > 0 THEN 
            SUM(vwap * volume) / SUM(volume)
        ELSE 
            AVG(close_price)
    END AS vwap,
    
    -- Window timing (15-minute windows)
    TUMBLE_START(window_end, INTERVAL '15' MINUTE) AS window_start,
    TUMBLE_END(window_end, INTERVAL '15' MINUTE) AS window_end,
    '15m' AS interval_type,
    CURRENT_TIMESTAMP AS processing_time

FROM ohlcv_5min_input
WHERE 
    interval_type = '5m'
    AND symbol IS NOT NULL
    AND volume >= 0
GROUP BY 
    symbol,
    TUMBLE(window_end, INTERVAL '15' MINUTE)
HAVING 
    COUNT(*) > 0  -- Ensure we have at least one 5-minute candle;
