from datetime import datetime

import pandas as pd

from src.utils.logger import logger
from src.utils.timescaledb_ops import TimescaleDBOps


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
        # Read from database
        db_ops = TimescaleDBOps()
        columns, data = db_ops.read_data(self.source)
        df = pd.DataFrame(data=data, columns=columns)
        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"] >= self.start_date) & (df["date"] <= self.end_date)]
        df = df.sort_values("date").reset_index(
            drop=True
        )  # sort by date ensure that the calculations are correct

        # EMA
        df["ema_13"] = df["close"].ewm(span=13, adjust=False).mean()
        df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()

        # Stochastic
        percentage_k_length = 5
        percentage_k_smoothing = 3
        percentage_d_length = 3
        lowest_low = df["low"].rolling(percentage_k_length).min()
        highest_high = df["high"].rolling(percentage_k_length).max()
        df["stochastic_percentage_k"] = (
            ((df["close"] - lowest_low) / (highest_high - lowest_low))
            .rolling(percentage_k_smoothing)
            .mean()
        )
        df["stochastic_percentage_d"] = (
            df["stochastic_percentage_k"].rolling(percentage_d_length).mean()
        )

        # MACD
        ema_12 = df["close"].ewm(span=12, adjust=False).mean()
        ema_26 = df["close"].ewm(span=26, adjust=False).mean()
        macd = ema_12 - ema_26
        macd_signal_line = macd.ewm(span=9, adjust=False).mean()
        macd_bar = macd - macd_signal_line
        df["macd"] = macd
        df["macd_signal_line"] = macd_signal_line
        df["macd_bar"] = macd_bar

        db_ops.batch_insert_data(
            schema=self.target.split(".")[0],
            table=self.target.split(".")[1],
            columns=df.columns.tolist(),
            data=df.values.tolist(),
            conflict_columns=["date", "pair"],
        )
        db_ops.close_connection()
        logger.info("Successfully run script!")
