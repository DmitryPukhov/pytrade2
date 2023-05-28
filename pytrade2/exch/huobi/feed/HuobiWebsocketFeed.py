import datetime
from datetime import timedelta
import logging
from typing import List, Dict

import huobi.model.market
from binance.websocket.spot.websocket_client import SpotWebsocketClient

import pandas as pd

from huobi.model.market import *
from huobi.client.market import MarketClient
from huobi.model.market.depth_entry import *


class HuobiWebsocketFeed:
    """
    Huobi price socket streams.
    """

    def __init__(self, config: dict, market_client: MarketClient):

        self.consumers = []
        self._log = logging.getLogger(self.__class__.__name__)
        self.tickers = config["pytrade2.tickers"].lower().split(",")
        self.__market_client = market_client
        # self.last_subscribe_time: datetime = datetime.datetime.min
        # self.subscribe_interval: timedelta = timedelta(seconds=60)

    def run(self):
        """
        Read data from web socket
        """

        self.__market_client.sub_pricedepth_bbo(symbols=",".join(self.tickers), callback=self.ticker_callback)
        self.__market_client.sub_pricedepth(symbols=",".join(self.tickers), depth_step="step1",
                                            callback=self.level2_callback)

    # def refresh_streams(self):
    #     """ Level2 stream stops after some time of work, refresh subscription """
    #     if datetime.datetime.utcnow() - self.last_subscribe_time >= self.subscribe_interval:
    #         for i, ticker in enumerate(self.tickers):
    #             self._log.debug(f"Refreshing subscription to data streams for {ticker}. "
    #                             f"Refresh interval: {self.subscribe_interval}")
    #             self.__market_client.sub_trade_detail(symbols="btcusdt", callback=lambda msg: print(msg),
    #                                                   error_handler=lambda e: print(f"Error: {e}"))

                # Bid/ask
                # self.market_client.sub_pricedepth(symbols=self.tickers, callback=self.ticker_callback)
                # Order book
                # print(self.market_client.get_market_tickers())

                # self.__market_client.sub_pricedepth(symbols=",".join(self.tickers), depth_step="step5", callback=self.level2_callback )

                # stream_name = f"{ticker.lower()}@depth"
                # self.market_client.live_subscribe(stream=stream_name, id=1, callback=self.level2_callback)
                # self.last_subscribe_time = datetime.datetime.utcnow()

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
        dt = datetime.datetime.utcnow()
        return {"datetime": dt,
                "symbol": tick.symbol,
                "bid": tick.bid,
                "bid_vol": tick.bidSize,
                "ask": tick.ask,
                "ask_vol": tick.askSize
                }

    def rawlevel2model(self, symbol:str, tick: PriceDepth):
        dt = pd.to_datetime(tick.ts, unit="ms")
        out = [{"datetime": dt, "symbol": symbol,
                "bid": entry.price, "bid_vol": entry.amount} for entry in tick.bids] + \
              [{"datetime": dt, "symbol": symbol,
                "ask": entry.price, "ask_vol": entry.amount} for entry in tick.asks]
        return out

    # def rawbidask2model(self, msg: Dict):
    #     """
    #     Convert raw binance data to model
    #     """
    #     out = []
    #     if msg["b"]:
    #         out.append({"datetime": datetime.datetime.utcnow(), "symbol": msg["s"], "bid": float(msg["b"]),
    #                     "bid_vol": float(msg["B"])})
    #     if msg["a"]:
    #         out.append({"datetime": datetime.datetime.utcnow(), "symbol": msg["s"], "ask": float(msg["a"]),
    #                     "ask_vol": float(msg["A"])})
    #     return out
