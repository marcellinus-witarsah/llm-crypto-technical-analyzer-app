import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from src.extractors.kraken_extractor import KrakenExtractor
from src.minio_ops import MinioOPS
from src.model.ohlc import OHLC
from src.utils.logger import logger


class DataPipeline:
    def __init__(
        self, pair: str, interval: int, start_date: datetime, end_date: datetime
    ):
        load_dotenv()
        self.pair = pair
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        logger.info(
            f"Data will be ingested from Kraken REST API to MinIO ({self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')})"
        )
        try:
            # =========================================================================
            # Request 4 hourly data from Kraken REST API
            # =========================================================================
            kraken_rest_api = KrakenExtractor()
            response = kraken_rest_api.get_ohlc_data(
                pair=self.pair,
                interval=self.interval,
                since=int(self.start_date.timestamp()),
            )

            # =========================================================================
            # Process 4 hourly data from Kraken REST API
            # =========================================================================
            data = response.get("result", {}).get("XXBTZUSD", [])
            data = [
                OHLC(
                    time=int(item[0]),
                    open=item[1],
                    high=item[2],
                    low=item[3],
                    close=item[4],
                    vwap=item[5],
                    volume=item[6],
                    count=int(item[7]),
                ).model_dump()
                for item in data
            ]  # use pydantic model for data validation and then convert it into a dictionary
            logger.info("Loaded OHLC Hourly Data from Kraken REST API")

            # =========================================================================
            # Create `staging` area before inserting into `MinIO`
            # =========================================================================
            file = f"{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}_{self.pair}_ohlc_{self.interval}.parquet"
            pd.DataFrame(data).to_parquet(
                f"tmp/{file}",
                index=False,
            )
            logger.info("Save OHLC Hourly Data from Kraken REST API")

            # =========================================================================
            # Copy file from `staging` area into `MinIO`
            # =========================================================================
            minio_ops = MinioOPS()
            minio_ops.create_bucket(os.getenv("BUCKET_NAME"))
            minio_ops.write_object(
                os.getenv("BUCKET_NAME"),
                destination_file=f"{file}",
                source_file=f"tmp/{file}",
            )
            logger.info("Data ingestion process completed successfully.")
        except Exception as e:
            logger.error(
                f"An error occurred during the data pipeline ingestion process: {e}"
            )
            return
