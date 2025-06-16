from datetime import datetime
from src.utils.logger import logger
from src.utils.timescaledb_ops import TimescaleDBOps
from psycopg2 import sql


class DataPipeline:
    def __init__(
        self, 
        source: str, 
        target: str, 
        start_date: datetime, 
        end_date: datetime,
    ):
        self.source = source
        self.target = target
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        db_ops = TimescaleDBOps()
        query = sql.SQL(
            """
            insert into {target} 
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
                FROM {source}
                WHERE time >= {start_date} AND time < {end_date}
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
            """
        ).format(
            source = sql.Identifier(self.source.split('.')[0], self.source.split('.')[1]),
            target = sql.Identifier(self.target.split('.')[0], self.target.split('.')[1]),
            start_date = sql.Literal(self.start_date),
            end_date = sql.Literal(self.end_date)
        )
        db_ops.execute_query(query)
        db_ops.close_connection()
        logger.info("Successfully run script!")


if __name__ == "__main__":
    pipeline = DataPipeline(
        source="bronze.ohlc",
        target="silver.ohlc_daily",
        start_date="2025-01-01",
        end_date="2025-06-15"
    )
    pipeline.run()
