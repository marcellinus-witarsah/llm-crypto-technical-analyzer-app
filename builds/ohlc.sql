-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Create bronze.ohlc
drop table if exists bronze.ohlc;
create table bronze.ohlc (
    time TIMESTAMPTZ,
    interval INTEGER,
    pair TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    count INTEGER,
    primary key (time, pair)
);
select create_hypertable('bronze.ohlc', by_range('time', INTERVAL '1 day'));


-- Create silver.ohlc_daily
drop table silver.ohlc_daily;
create table silver.ohlc_daily (
    date DATE,
    pair TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    count INTEGER,
    primary key (date, pair)
);
select create_hypertable('silver.ohlc_daily', by_range('date', INTERVAL '1 day'));

-- Create silver.ohlc_weekly
drop table silver.ohlc_weekly;
create table silver.ohlc_weekly (
    date DATE,
    pair TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    count INTEGER,
    primary key (date, pair)
);
select create_hypertable('silver.ohlc_weekly', by_range('date', INTERVAL '1 week'));


-- Create silver.ohlc_monthly
drop table silver.ohlc_monthly;
create table silver.ohlc_monthly (
    date DATE,
    pair TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    count INTEGER,
    primary key (date, pair)
);

-- Create gold.ohlc_ta_daily
select create_hypertable('silver.ohlc_monthly', by_range('date', INTERVAL '1 month'));
drop table if exists gold.ohlc_ta_daily;
create table if not exists gold.ohlc_ta_daily(
    date DATE,
    pair TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    count INTEGER,
    ema_13 DOUBLE PRECISION,
    ema_21 DOUBLE PRECISION,
    stochastic_percentage_k DOUBLE PRECISION,
    stochastic_percentage_d DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal_line DOUBLE PRECISION,
    macd_bar DOUBLE PRECISION,
    primary key (date, pair)
);
select create_hypertable('gold.ohlc_ta_daily', by_range('date', INTERVAL '1 day'));

-- Create gold.ohlc_ta_weekly
drop table if exists gold.ohlc_ta_weekly;
create table if not exists gold.ohlc_ta_weekly(
    date DATE,
    pair TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    count INTEGER,
    ema_13 DOUBLE PRECISION,
    ema_21 DOUBLE PRECISION,
    stochastic_percentage_k DOUBLE PRECISION,
    stochastic_percentage_d DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal_line DOUBLE PRECISION,
    macd_bar DOUBLE PRECISION,
    primary key (date, pair)
);
select create_hypertable('gold.ohlc_ta_weekly', by_range('date', INTERVAL '1 week'));

-- Create gold.ohlc_ta_monthly
drop table if exists gold.ohlc_ta_monthly;
create table if not exists gold.ohlc_ta_monthly(
    date DATE,
    pair TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    count INTEGER,
    ema_13 DOUBLE PRECISION,
    ema_21 DOUBLE PRECISION,
    stochastic_percentage_k DOUBLE PRECISION,
    stochastic_percentage_d DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal_line DOUBLE PRECISION,
    macd_bar DOUBLE PRECISION,
    primary key (date, pair)
);
select create_hypertable('gold.ohlc_ta_monthly', by_range('date', INTERVAL '1 month'));