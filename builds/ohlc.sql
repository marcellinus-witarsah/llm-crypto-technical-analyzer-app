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

-- Query for inserting from bronze.ohlc to silver.ohlc_daily
select * from bronze.ohlc order by time desc;
insert into silver.ohlc_daily 
(
	SELECT
        time_bucket('1 day', time at time zone 'Asia/Jakarta') AS date,
        pair,
        first(open, time) AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        last(close, time) AS close,
        SUM(volume) AS volume,
        SUM(count) AS count
	FROM bronze.ohlc
	where time_bucket('1 day', time at time zone 'Asia/Jakarta') >= '2025-06-01' 
		and time_bucket('1 day', time at time zone 'Asia/Jakarta')<= '2025-06-15'
	GROUP BY date, pair
)
ON CONFLICT (date,pair)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    count = EXCLUDED.count
;

insert into silver.ohlc_weekly
(
	SELECT
        time_bucket('1 week', time at time zone 'Asia/Jakarta') AS date,
        pair,
        first(open, time) AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        last(close, time) AS close,
        SUM(volume) AS volume,
        SUM(count) AS count
	FROM bronze.ohlc
	where time_bucket('1 week', time at time zone 'Asia/Jakarta') >= '2025-01-01' 
		and time_bucket('1 week', time at time zone 'Asia/Jakarta')<= '2025-06-01'
	GROUP BY date, pair
)
ON CONFLICT (date,pair)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    count = EXCLUDED.count
;

insert into silver.ohlc_monthly 
(
	SELECT
        time_bucket('1 month', time at time zone 'Asia/Jakarta') AS date,
        pair,
        first(open, time) AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        last(close, time) AS close,
        SUM(volume) AS volume,
        SUM(count) AS count
	FROM bronze.ohlc
	where time_bucket('1 month', time at time zone 'Asia/Jakarta') >= '2025-01-01' 
	and time_bucket('1 month', time at time zone 'Asia/Jakarta')<= '2025-06-01'
	GROUP BY date, pair
)
ON CONFLICT (date,pair)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    count = EXCLUDED.count
;




