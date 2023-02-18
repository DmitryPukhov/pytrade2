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
            # todo: find a way to get stock last price
            client.book_ticker(id=i, symbol=ticker, callback=self.ticker_callback)
            #client.live_subscribe(stream="btcusdt@depth", id=1, callback=self.level2_callback)
        client.join()

    def level2_callback(self, msg):
        for consumer in [c for c in self.consumers if hasattr(c, 'on_level2')]:
            consumer.on_level2(self.rawbidask2model(msg))

    def ticker_callback(self, msg):
        if "result" in msg and not msg["result"]:
            return

        for consumer in [c for c in self.consumers if hasattr(c, 'on_ticker')]:
            consumer.on_ticker(self.rawticker2model(msg))

    def rawticker2model(self, msg: Dict):
        # todo: convert price+vol ticker to model
        return msg

    def rawbidask2model(self, msg: Dict):
        """
        Convert raw binance data to model
        """
        out = []
        if msg["b"]:
            out.append({"datetime": datetime.datetime.now(), "symbol": msg["s"], "bid": msg["b"], "bid_vol": msg["B"]})
        if msg["a"]:
            out.append({"datetime": datetime.datetime.now(), "symbol": msg["s"], "ask": msg["a"], "ask_vol": msg["A"]})
        return out
        # return {"datetime": datetime.datetime.now(), "symbol": msg["s"], "bid": msg["b"], "bid_qty": msg["B"],
        #         "ask": msg["a"], "ask_qty": msg["A"]}
