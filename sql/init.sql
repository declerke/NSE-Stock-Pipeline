CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.nse_prices (
    id          SERIAL PRIMARY KEY,
    ticker      VARCHAR(20)    NOT NULL,
    trade_date  DATE           NOT NULL,
    open        NUMERIC(12, 4),
    high        NUMERIC(12, 4),
    low         NUMERIC(12, 4),
    close       NUMERIC(12, 4),
    volume      BIGINT,
    loaded_at   TIMESTAMP      DEFAULT NOW(),
    CONSTRAINT uq_ticker_date UNIQUE (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_nse_prices_ticker ON raw.nse_prices (ticker);
CREATE INDEX IF NOT EXISTS idx_nse_prices_date   ON raw.nse_prices (trade_date);

CREATE DATABASE airflow_db;
