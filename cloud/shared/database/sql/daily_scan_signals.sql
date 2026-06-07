-- Canonical DDL for the scanner staging table.
-- Also embedded in batch_layer/database/schemas/schema_init.sql (Section 5).

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

CREATE INDEX IF NOT EXISTS idx_daily_scan_signals_date
ON daily_scan_signals(scan_date);
