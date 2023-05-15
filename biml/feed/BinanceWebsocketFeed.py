from datetime import timedelta, datetime
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
        self.client: SpotWebsocketClient = None
        self.feed_timeout: timedelta = timedelta(seconds=60)
        self._log.info(f"Feeding tickers: {self.tickers}, reconnection timeout: {self.feed_timeout}")
        self.last_ticker_time: datetime = datetime.min
        self.last_level2_time: datetime = datetime.min

    def run(self):
        """
        Read data from web socket
        """
        self.client = SpotWebsocketClient()
        self.client.start()
        self.ensure_streams()

        self.client.join()

    #
    def ensure_streams(self):
        """ Start streams in the very beginning or if timeout elapsed. """

        # Start or refresh tickers
        now = datetime.utcnow()
        if (now == datetime.min) or (now - self.last_ticker_time > self.feed_timeout):
            if self.last_ticker_time > datetime.min:
                self._log.info(f"Ticker stream looks broken, timeout {self.feed_timeout} passed.")
            for i, ticker in enumerate(self.tickers):
                stream_name = "{}@bookTicker".format(ticker.lower())
                self.client.stop_socket(stream_name)  # Stop or no action
                self._log.info(f"Starting ticker stream: {stream_name}")
                self.client.live_subscribe(stream=stream_name, id=len(self.tickers) + i, callback=self.ticker_callback)
            self.last_ticker_time = now

        # Start or refresh level2
        if (now == datetime.min) or (now - self.last_level2_time > self.feed_timeout):
            if self.last_level2_time > datetime.min:
                self._log.info(f"Level2 stream looks broken, timeout {self.feed_timeout} passed.")
            for i, ticker in enumerate(self.tickers):
                stream_name = f"{ticker.lower()}@depth"
                self.client.stop_socket(stream_name)  # Stop or no action
                self._log.info(f"Starting level2 stream: {stream_name}")
                self.client.live_subscribe(stream=stream_name, id=len(self.tickers) + i, callback=self.level2_callback)
            self.last_level2_time = now

    def level2_callback(self, msg):
        self.last_level2_time = datetime.utcnow()
        if "result" in msg and not msg["result"]:
            return
        try:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_level2')]:
                consumer.on_level2(self.rawlevel2model(msg))

            # Refresh stream subscriptions if timeout
            self.ensure_streams()
        except Exception as e:
            self._log.error(e)

    def ticker_callback(self, msg):
        self.last_ticker_time = datetime.utcnow()
        if "result" in msg and not msg["result"]:
            return
        try:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_ticker')]:
                consumer.on_ticker(self.rawticker2model(msg))

            # Refresh stream subscriptions if timeout
            self.ensure_streams()
        except Exception as e:
            self._log.error(e)

    def rawticker2model(self, msg: Dict) -> Dict:
        return {"datetime": datetime.utcnow(),
                "symbol": msg["s"],
                "bid": float(msg["b"]), "bid_vol": float(msg["B"]),
                "ask": float(msg["a"]), "ask_vol": float(msg["A"]),
                }

    def rawlevel2model(self, msg: Dict):
        # dt=pd.to_datetime(msg["E"], unit='ms')
        dt = datetime.utcnow()  # bid/ask has no datetime field, so use this machine's time
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
            out.append({"datetime": datetime.utcnow(), "symbol": msg["s"], "bid": float(msg["b"]),
                        "bid_vol": float(msg["B"])})
        if msg["a"]:
            out.append({"datetime": datetime.utcnow(), "symbol": msg["s"], "ask": float(msg["a"]),
                        "ask_vol": float(msg["A"])})
        return out
