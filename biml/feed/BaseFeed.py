class BaseFeed:
    """
    Base class for price data feed. Read data, provide pandas dataframes with that data
    """

    bid_ask_columns = ["datetime", "symbol", "bid", "bid_vol", "ask", "ask_vol"]

    def __init__(self):
        self.consumers = []

    def read(self):
        """
        Read data to pandas
        """
        pass
