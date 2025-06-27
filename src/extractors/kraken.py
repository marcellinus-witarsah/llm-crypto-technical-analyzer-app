import requests

from src.utils.logger import logger


class KrakenRestAPI:
    def __init__(self):
        self.base_url = "https://api.kraken.com/0"

    def get_ohlc_data(self, pair: str, interval: int, since: int) -> dict:
        """
        Fetch OHLC data for a given trading pair and interval.

        :param pair: Trading pair (e.g., 'XXBTZUSD').
        :param interval: Time interval in minutes (1, 5, 15, 30, 60, 240, 1440, 10080, 21600).
        :param since: Timestamp in seconds to fetch data since.
        :return: JSON response containing OHLC data.
        """
        url = f"{self.base_url}/public/OHLC"
        params = {"pair": pair, "interval": interval, "since": since}
        try:
            response = requests.get(url, params=params)
        except Exception as e:
            logger.error(f"Error fetching OHLC data: {e}")
            return None
        return response.json()
