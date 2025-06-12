create table public.ohlc_four_hourly (
    time TIMESTAMPTZ,
    pair TEXT,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    count INTEGER,
    primary key (time)
)