import re
from datetime import datetime
from typing import Dict, List

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from exch.huobi.hbdm.feed.HuobiFeedBase import HuobiFeedBase


class HuobiWebSocketFeedHbdm(HuobiFeedBase):
    """
    Huobi derivatives market web socket.
    """
    ticker_template = "{ticker}"

    def __init__(self, config: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        super().__init__(config, rest_client, ws_client)

        templates = config.get("pytrade2.exchange.feed.huobi.websocket.sub.topics.template",
                               f"market.{self.ticker_template}.bbo,market.{self.ticker_template}.depth.step6")
        templates = templates.split(",")

        self.subscribe_topics = self.topics_of(templates, self.tickers)
        self.sub_events()

    @staticmethod
    def topics_of(templates: list[str], tickers: list[str]):
        """ Topics to subscribe websocket"""
        topics = []

        # Apply each template to each ticker
        for template in templates:
            for ticker in tickers:
                topic = template.strip().replace("{ticker}", ticker)
                topics.append(topic)
        return topics

    @staticmethod
    def is_bidask(ch):
        """ If channel name is bidask like market.BTC-USDT.bbo """
        return re.fullmatch("market\\..*\\.bbo", ch)

    @staticmethod
    def is_level2(ch):
        """ Is channel name is level2 like market.BTC-USDT.depth.step1"""
        # return re.fullmatch("market\\..*\\.depth\\.step\\d+", ch)
        return re.fullmatch("market\\..*\\.depth\\..+", ch)

    def sub_events(self):
        """ Subscribe to websocket feeds: bid/ask, level2 if configured"""

        for topic in self.subscribe_topics:
            self._logger.info(f"Subscribing to websocket feed {topic}")
            self._client.add_consumer(topic, {"sub": topic}, self)
        self._logger.info("Subscribed to all")

    def on_socket_data(self, topic, msg):
        """ Got subscribed data from socket"""
        try:
            # Topic is a channel like "market.BTC-USDT.bbo"
            if not topic:
                return
            # If bidask or level2 received
            if self.is_bidask(topic):
                bidask = self.rawticker2model(msg["tick"])
                for consumer in [c for c in self.consumers if hasattr(c, 'on_ticker')]:
                    consumer.on_ticker(bidask)
            elif self.is_level2(topic):
                l2 = self.rawlevel2model(msg["tick"])
                for consumer in [c for c in self.consumers if hasattr(c, 'on_level2')]:
                    consumer.on_level2(l2)
        except Exception as e:
            self._logger.error(e)

    @staticmethod
    def rawlevel2model(tick: dict) -> [{}]:
        # dt = datetime.utcfromtimestamp(tick["ts"] / 1000)
        dt = datetime.utcnow()
        ticker = HuobiWebSocketFeedHbdm.ticker_of_ch(tick["ch"])
        bids: List[Dict] = [{"datetime": dt, "symbol": ticker, "bid": float(price), "bid_vol": float(vol)}
                            for price, vol in tick["bids"]]
        asks: List[Dict] = [{"datetime": dt, "symbol": ticker, "ask": float(price), "ask_vol": float(vol)}
                            for price, vol in tick["asks"]]
        return bids + asks

    @staticmethod
    def rawticker2model(tick: dict) -> Dict:
        # dt = datetime.utcfromtimestamp(tick["ts"] / 1000)
        dt = datetime.utcnow()
        ticker = HuobiWebSocketFeedHbdm.ticker_of_ch(tick["ch"])
        return {"datetime": dt,
                "symbol": ticker,
                "bid": tick["bid"][0],
                "bid_vol": tick["bid"][1],
                "ask": tick["ask"][0],
                "ask_vol": tick["ask"][1]
                }
