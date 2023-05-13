import datetime
import logging
from typing import List, Dict

import pandas as pd
from binance.websocket.spot.websocket_client import SpotWebsocketClient


class BinanceWebsocketFeed:
    """
    Binance price data feed. Read data from binance, provide pandas dataframes with that data
    """

    bid_ask_columns = ["datetime", "symbol", "bid", "bid_vol", "ask", "ask_vol"]

    def __init__(self, tickers: List[str]):
        self.consumers = []
        self._log = logging.getLogger(self.__class__.__name__)
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
            client.live_subscribe(stream="btcusdt@depth", id=1, callback=self.level2_callback)
            # client.live_subscribe(stream="btcusdt@bookTicker", id=1, callback=self.ticker_callback)
        client.join()

    def level2_callback(self, msg):
        if "result" in msg and not msg["result"]:
            return
        try:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_level2')]:
                consumer.on_level2(self.rawlevel2model(msg))
        except Exception as e:
            self._log.error(e)

    def ticker_callback(self, msg):
        if "result" in msg and not msg["result"]:
            return
        try:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_ticker')]:
                consumer.on_ticker(self.rawticker2model(msg))
        except Exception as e:
            self._log.error(e)

    def rawticker2model(self, msg: Dict) -> Dict:
        return {"datetime": datetime.datetime.utcnow(),
                "symbol": msg["s"],
                "bid": float(msg["b"]), "bid_vol": float(msg["B"]),
                "ask": float(msg["a"]), "ask_vol": float(msg["A"]),
                }

    def rawlevel2model(self, msg: Dict):
        # dt=pd.to_datetime(msg["E"], unit='ms')
        dt = datetime.datetime.utcnow()  # bid/ask has no datetime field, so use this machine's time
        out = [{"datetime": dt, "symbol": msg["s"],
                "bid": float(price), "bid_vol": float(vol)} for price, vol in msg['b']] + \
              [{"datetime": dt, "symbol": msg["s"],
                "ask": float(price), "ask_vol": float(vol)} for price, vol in msg['a']]
        return out

    def rawbidask2model(self, msg: Dict):
        """
        Convert raw binance data to model
        """
        out = []
        if msg["b"]:
            out.append({"datetime": datetime.datetime.utcnow(), "symbol": msg["s"], "bid": float(msg["b"]),
                        "bid_vol": float(msg["B"])})
        if msg["a"]:
            out.append({"datetime": datetime.datetime.utcnow(), "symbol": msg["s"], "ask": float(msg["a"]),
                        "ask_vol": float(msg["A"])})
        return out
