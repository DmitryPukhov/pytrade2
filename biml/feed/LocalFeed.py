from typing import List
from biml.feed.BaseFeed import BaseFeed
from biml.feed.TickerInfo import TickerInfo


class LocalFeed(BaseFeed):
    """
    Read data from local folder, provide pandas dataframes with that data
    """

    def __init__(self, data_dir: str, tickers: List[TickerInfo]):
        super().__init__(tickers=tickers)
        self.data_dir = data_dir

    def read(self):
        """
        Read data from local folder to pandas
        """
        pass
        # todo: implement reading from local folder
