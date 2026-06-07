-- Kinesis Analytics Flink SQL: Vegas Channel Signal Detection
-- Purpose: Detect Vegas Channel breakout and support/resistance signals
-- Input: Multi-timeframe OHLCV data (15m, 1h, 4h)
-- Output: Vegas Channel trading signals

-- Vegas Channel Strategy:
-- - Uses 12 EMA (fast) and 144 EMA (slow) as channel boundaries
-- - Signals generated on breakouts above/below channel
-- - Considers volume confirmation and trend strength

-- Create input stream for 15-minute data (primary timeframe)
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

-- Create output stream for Vegas Channel signals
CREATE TABLE vegas_channel_signals (
    symbol VARCHAR(10),
    signal_type VARCHAR(20),          -- 'vegas_breakout_long', 'vegas_breakout_short', 'vegas_support', 'vegas_resistance'
    signal_strength DOUBLE,           -- 0.0 to 1.0 confidence score
    trigger_price DOUBLE,             -- Price that triggered the signal
    ema_12 DOUBLE,                    -- Fast EMA value
    ema_144 DOUBLE,                   -- Slow EMA value
    volume_ratio DOUBLE,              -- Current volume vs average volume
    signal_timestamp TIMESTAMP(3),
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    metadata VARCHAR(500)             -- JSON metadata with additional context
) WITH (
    'connector' = 'kinesis',
    'stream' = 'trading-signals',
    'aws.region' = 'ca-west-1',
    'format' = 'json'
);

-- Calculate EMAs and generate Vegas Channel signals
INSERT INTO vegas_channel_signals
SELECT 
    symbol,
    
    -- Determine signal type based on price action relative to EMAs
    CASE 
        WHEN close_price > ema_12 AND ema_12 > ema_144 AND 
             LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY window_end) <= 
             LAG(ema_12, 1) OVER (PARTITION BY symbol ORDER BY window_end) 
        THEN 'vegas_breakout_long'
        
        WHEN close_price < ema_12 AND ema_12 < ema_144 AND 
             LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY window_end) >= 
             LAG(ema_12, 1) OVER (PARTITION BY symbol ORDER BY window_end)
        THEN 'vegas_breakout_short'
        
        WHEN close_price BETWEEN ema_144 * 0.999 AND ema_144 * 1.001 AND ema_12 > ema_144
        THEN 'vegas_support'
        
        WHEN close_price BETWEEN ema_12 * 0.999 AND ema_12 * 1.001 AND ema_12 < ema_144
        THEN 'vegas_resistance'
        
        ELSE NULL
    END AS signal_type,
    
    -- Calculate signal strength based on multiple factors
    LEAST(1.0, GREATEST(0.0, 
        (ABS(close_price - ema_12) / ema_12) * 100 * 2 +  -- Price distance from EMA
        LEAST(0.3, volume_ratio * 0.1) +                   -- Volume confirmation
        LEAST(0.2, ABS(ema_12 - ema_144) / ema_144 * 10)   -- Channel width
    )) AS signal_strength,
    
    close_price AS trigger_price,
    ema_12,
    ema_144,
    volume_ratio,
    window_end AS signal_timestamp,
    window_start,
    window_end,
    
    -- Metadata with additional context
    CONCAT(
        '{"channel_width":', ROUND(ABS(ema_12 - ema_144) / ema_144 * 100, 4),
        ',"price_vs_vwap":', ROUND((close_price - vwap) / vwap * 100, 4),
        ',"volume_rank":', CASE WHEN volume_ratio > 2.0 THEN 'high' 
                               WHEN volume_ratio > 1.5 THEN 'above_avg'
                               WHEN volume_ratio < 0.5 THEN 'low'
                               ELSE 'normal' END,
        '}'
    ) AS metadata

FROM (
    SELECT 
        symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        vwap,
        window_start,
        window_end,
        
        -- Calculate 12-period EMA (fast)
        AVG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY window_end 
            RANGE BETWEEN INTERVAL '180' MINUTE PRECEDING AND CURRENT ROW
        ) AS ema_12,
        
        -- Calculate 144-period EMA (slow) - approximation using longer window
        AVG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY window_end 
            RANGE BETWEEN INTERVAL '2160' MINUTE PRECEDING AND CURRENT ROW
        ) AS ema_144,
        
        -- Calculate volume ratio (current vs 20-period average)
        volume / NULLIF(
            AVG(volume) OVER (
                PARTITION BY symbol 
                ORDER BY window_end 
                RANGE BETWEEN INTERVAL '300' MINUTE PRECEDING AND CURRENT ROW
            ), 0
        ) AS volume_ratio
        
    FROM ohlcv_15min_input
    WHERE interval_type = '15m'
      AND symbol IS NOT NULL
      AND close_price > 0
) WITH_INDICATORS

WHERE signal_type IS NOT NULL
  AND signal_strength > 0.3  -- Only emit signals with minimum confidence
