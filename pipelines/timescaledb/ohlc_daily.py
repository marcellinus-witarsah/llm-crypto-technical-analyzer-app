from datetime import datetime
from utils.logger import logger
from schemas.ohlc import OHLCHourly
from source.kraken_rest_api import KrakenRestAPI
import pandas as pd
from minio_object_storage.minio_ops import MinioOPS


class DataPipeline:
    def __init__(self):
        self.start_date = datetime(2009, 1, 1)
        # self.current_date = datetime.now()
        self.current_date = datetime(2025, 6, 7)

    def run(self):
        logger.info(
            f"Transform OHLC Hourly Data Into Daily Data from {self.start_date.strftime('%Y-%m-%d')} to {self.current_date.strftime('%Y-%m-%d')}"
        )
        try:
            logger.info("Ingesting data from Kraken REST API...")
            # # Here you would typically call the Kraken REST API to fetch data
            # kraken_rest_api = KrakenRestAPI()
            # response = kraken_rest_api.get_ohlc_data(pair="XXBTZUSD", interval=60, since=int(self.start_date.timestamp()))
            # data = response.get('result', {}).get('XXBTZUSD', [])

            # data = [
            #     DailyOHLC(
            #         time=int(item[0]),
            #         open=item[1],
            #         high=item[2],
            #         low=item[3],
            #         close=item[4],
            #         vwap=item[5],
            #         volume=item[6],
            #         count=int(item[7])
            #     ).model_dump()
            #     for item in data
            # ]  # use pydantic model for data validation and then convert it into a dictionary
            # logger.info("Loaded OHLC Hourly Data from Kraken REST API")

            # # Create file to
            # pd.DataFrame(data).to_parquet(f"tmp/{self.current_date.strftime('%Y%m%d')}_ohlc_hourly.parquet", index=False)
            # logger.info("Save OHLC Hourly Data from Kraken REST API")

            # Simulate writing data to object storage
            minio_ops = MinioOPS("localhost:9000", "admin", "adminpassword")
            minio_ops.create_bucket("bitcoin-ohlc")
            minio_ops.write_object(
                "bitcoin-ohlc",
                destination_file=f"/raw/{self.current_date.strftime('%Y%m%d')}_ohlc_hourly.parquet",
                source_file=f"tmp/{self.current_date.strftime('%Y%m%d')}_ohlc_hourly.parquet",
            )

            logger.info("Data ingestion process completed successfully.")
        except Exception as e:
            logger.error(
                f"An error occurred during the data pipeline ingestion process: {e}"
            )
            return


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run()
