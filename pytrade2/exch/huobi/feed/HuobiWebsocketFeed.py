import logging
from datetime import datetime
from typing import Dict

import pandas as pd
from huobi.client.market import MarketClient
from huobi.model.market import *
from huobi.utils import PrintBasic


class HuobiWebsocketFeed:
    """
    Huobi price socket streams.
    """

    def __init__(self, config: dict, market_client: MarketClient):

        self.consumers = []
        self._log = logging.getLogger(self.__class__.__name__)
        self.tickers = config["pytrade2.tickers"].lower().split(",")
        self.__market_client = market_client

    def run(self):
        """
        Read data from web socket
        """
        symbols = ",".join(self.tickers)
        self.__market_client.sub_pricedepth_bbo(symbols=symbols,
                                                callback=self.ticker_callback,
                                                error_handler=self.error_callback)
        self.__market_client.sub_pricedepth(symbols=symbols,
                                            depth_step="step1",
                                            callback=self.level2_callback,
                                            error_handler=self.error_callback)

    def error_callback(self, msg):
        self._log.error(f"Web socket price depth subscription error: {msg}")

    def level2_callback(self, msg: PriceDepthEvent):
        try:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_level2')]:
                consumer.on_level2(self.rawlevel2model(self.tickers[-1], msg.tick))
        except Exception as e:
            self._log.error(e)

    def ticker_callback(self, event: PriceDepthBboEvent):
        try:
            for consumer in [c for c in self.consumers if hasattr(c, 'on_ticker')]:
                consumer.on_ticker(self.rawticker2model(event.tick))
        except Exception as e:
            self._log.error(e)

    def rawticker2model(self, tick: PriceDepthBbo) -> Dict:
        dt = datetime.utcnow()
        return {"datetime": dt,
                "symbol": tick.symbol,
                "bid": tick.bid,
                "bid_vol": tick.bidSize,
                "ask": tick.ask,
                "ask_vol": tick.askSize
                }

    def rawlevel2model(self, symbol: str, tick: PriceDepth):
        # dt = pd.to_datetime(tick.ts, unit="ms")
        dt = datetime.utcnow()
        out = [{"datetime": dt, "symbol": symbol,
                "bid": entry.price, "bid_vol": entry.amount} for entry in tick.bids] + \
              [{"datetime": dt, "symbol": symbol,
                "ask": entry.price, "ask_vol": entry.amount} for entry in tick.asks]
        return out
