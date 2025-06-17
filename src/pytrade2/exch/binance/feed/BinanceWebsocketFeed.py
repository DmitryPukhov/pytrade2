import datetime
import logging
from datetime import timedelta
from typing import Dict

from binance.websocket.spot.websocket_client import SpotWebsocketClient


class BinanceWebsocketFeed:
    """
    Binance price data feed. Read data from binance, provide pandas dataframes with that data
    """

    def __init__(self, config: dict, websocket_client: SpotWebsocketClient):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.consumers = []
        self.tickers = config["pytrade2.tickers"].split(",")
        self.websocket_client = websocket_client
        self.last_subscribe_time: datetime = datetime.datetime.min
        self.subscribe_interval: timedelta = timedelta(seconds=60)

    def run(self):
        """
        Read data from web socket
        """
        self.websocket_client.start()

        # Subscribe to streams
        self.refresh_streams()

        self.websocket_client.join()

    def refresh_streams(self):
        """ Level2 stream stops after some time of work, refresh subscription """
        if datetime.datetime.utcnow() - self.last_subscribe_time >= self.subscribe_interval:
            for i, ticker in enumerate(self.tickers):
                self._logger.debug(f"Refreshing subscription to data streams for {ticker}. "
                                f"Refresh interval: {self.subscribe_interval}")
                # Bid/ask
                self.websocket_client.book_ticker(id=i, symbol=ticker, callback=self.ticker_callback)
                # Order book
                stream_name = f"{ticker.lower()}@depth"
                self.websocket_client.live_subscribe(stream=stream_name, id=1, callback=self.level2_callback)
                self.last_subscribe_time = datetime.datetime.utcnow()

    def level2_callback(self, msg):
        if "result" in msg and not msg["result"]:
            return
        try:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_level2')]:
                consumer.on_level2(self.rawlevel2model(msg))
            # Refresh stream subscriptions if refresh interval passed
            self.refresh_streams()
        except Exception as e:
            self._logger.error(e)

    def ticker_callback(self, msg):
        if "result" in msg and not msg["result"]:
            return
        try:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_ticker')]:
                consumer.on_ticker(self.rawticker2model(msg))
        except Exception as e:
            self._logger.error(e)

    @staticmethod
    def rawticker2model(msg: Dict) -> Dict:
        return {"datetime": datetime.datetime.utcnow(),
                "symbol": msg["s"],
                "bid": float(msg["b"]), "bid_vol": float(msg["B"]),
                "ask": float(msg["a"]), "ask_vol": float(msg["A"]),
                }

    @staticmethod
    def rawlevel2model(msg: Dict):
        # dt=pd.to_datetime(msg["E"], unit='ms')
        dt = datetime.datetime.utcnow()  # bid/ask has no datetime field, so use this machine's time
        out = [{"datetime": dt, "symbol": msg["s"],
                "bid": float(price), "bid_vol": float(vol)} for price, vol in msg['b']] + \
              [{"datetime": dt, "symbol": msg["s"],
                "ask": float(price), "ask_vol": float(vol)} for price, vol in msg['a']]
        return out

    @staticmethod
    def rawbidask2model(msg: Dict):
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
