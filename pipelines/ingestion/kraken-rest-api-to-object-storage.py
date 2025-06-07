from datetime import datetime
from utils.logger import logger
from data.raw.daily_ohlc import DailyOHLC
from source.kraken_rest_api import KrakenRestAPI
import pandas as pd

class DataPipeline:
    def __init__(self):
        self.start_date = "2009-01-01"
        self.end_date = datetime.now().strftime("%Y-%m-%d")
    def run(self):
        logger.info("Starting data pipeline ingestion process...")
        logger.info(f"Data will be ingested from {self.start_date} to {self.end_date}")
        try:
            logger.info("Ingesting data from Kraken REST API...")
            
            # Here you would typically call the Kraken REST API to fetch data
            kraken_rest_api = KrakenRestAPI()
            response = kraken_rest_api.get_ohlc_data(pair="XXBTZUSD", interval=1440, since=int(datetime.strptime(self.start_date, "%Y-%m-%d").timestamp()))
            data = response.get('result', {}).get('XXBTZUSD', [])
            
            data = [
                DailyOHLC(
                    time=int(item[0]),
                    open=item[1],
                    high=item[2],
                    low=item[3],
                    close=item[4],
                    vwap=item[5],
                    volume=item[6],
                    count=int(item[7])
                ).model_dump() 
                for item in data
            ]  # use pydantic model for data validation and then convert it into a dictionary
            
            df = pd.DataFrame(data)

            # Simulate writing data to object storage
            logger.info("Writing data to object storage...")
            
            
            logger.info("Data ingestion process completed successfully.")
        except Exception as e:
            logger.error(f"An error occurred during the data pipeline ingestion process: {e}")
            return
        
if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run()