import logging


class BaseFeed:
    """
    Base class for price data feed. Read data, provide pandas dataframes with that data
    """

    bid_ask_columns = ["datetime", "symbol", "bid", "bid_vol", "ask", "ask_vol"]

    def __init__(self):
        self.consumers = []
        self._log = logging.getLogger(self.__class__.__name__)


    def read(self):
        """
        Read data to pandas
        """
        pass
