import datetime
from typing import List, Dict
from binance.websocket.spot.websocket_client import SpotWebsocketClient
from feed.BaseFeed import BaseFeed


class BinanceWebsocketFeed(BaseFeed):
    """
    Binance price data feed. Read data from binance, provide pandas dataframes with that data
    """

    def __init__(self, tickers: List[str]):
        super().__init__()
        self.tickers = tickers

    def run(self):
        """
        Read data from web socket
        """
        client = SpotWebsocketClient()
        client.start()
        # Request the data maybe for multiple assets
        for i, ticker in enumerate(self.tickers):
            client.book_ticker(id=i, symbol=ticker, callback=self.ticker_callback)
        client.join()

    def ticker_callback(self, msg):
        if "result" in msg and not msg["result"]:
            return
        for consumer in [c for c in self.consumers if hasattr(c, 'on_bid_ask')]:
            consumer.on_bid_ask(self.raw2model(msg))

    def raw2model(self, msg: Dict):
        """
        Convert raw binance data to model
        """
        return {"datetime": datetime.datetime.now(), "symbol": msg["s"], "bid": msg["b"], "bid_qty": msg["B"],
                "ask": msg["a"], "ask_qty": msg["A"]}
