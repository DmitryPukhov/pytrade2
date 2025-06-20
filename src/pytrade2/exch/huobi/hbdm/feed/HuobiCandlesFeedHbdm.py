import logging
from datetime import datetime, timezone, tzinfo, timedelta

import pandas as pd

from pytrade2.exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from pytrade2.exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from pytrade2.exch.huobi.hbdm.feed.HuobiFeedBase import HuobiFeedBase


class HuobiCandlesFeedHbdm(HuobiFeedBase):
    """
    Huobi candles feed on Huobi derivatives market.
    """

    def __init__(self, config: dict, rest_client: HuobiRestClient, ws_client: HuobiWebSocketClient):
        self._logger = logging.getLogger(self.__class__.__name__)
        super().__init__(config, rest_client, ws_client)
        self.periods = [s.strip() for s in str(self.config["pytrade2.feed.candles.periods"]).split(",")]
        #self.counts = [int(s) for s in str(self.config["pytrade2.feed.candles.counts"]).split(",")]
        self.candles = {}
        #self.sub_events()

    def sub_events(self):
        self._logger.info(f"Subscribing to {','.join(self.periods)} candles of {','.join(self.tickers)}")
        for ticker in self.tickers:
            for period in self.periods:
                topic = f"market.{ticker}.kline.{period}"
                self._client.add_consumer(topic, {"sub": topic}, self)

    def on_socket_data(self, topic, msg):
        """ Got subscribed data from socket"""
        try:
            # Convert
            candle = self.raw_socket_msg_to_candle(msg)
            # Produce to consumers
            for consumer in self.consumers:
                consumer.on_candle(candle)
        except Exception as e:
            self._logger.error(e)

    @staticmethod
    def raw_socket_msg_to_candle(msg):
        ticker = HuobiCandlesFeedHbdm.ticker_of_ch(msg["ch"])
        period = HuobiCandlesFeedHbdm.period_of_ch(msg["ch"])
        # dt = datetime.fromtimestamp(msg["ts"] / 1000, tz=timezone.utc)
        # dt = datetime.fromtimestamp(msg["ts"] / 1000)
        return HuobiCandlesFeedHbdm.rawcandle2model(ticker, period, msg["tick"])

    def run(self):
        super().run()

    def read_candles(self, ticker: str, interval: str, limit: int, from_: datetime = None, to: datetime = None):
        """ Read candles from Huobi """

        # Get candles from huobi rest
        path = "/linear-swap-ex/market/history/kline"
        params = {"contract_code": ticker,
                  "period": interval,
                  }

        if limit:
            params["size"] = limit
        if from_:
            params["from"] = int(from_.timestamp())
        if to:
            params["to"] = int(to.timestamp())
        res = self.rest_client.get(path, params)

        # Convert
        candles = self.rawcandles2list(res)
        return candles

    @staticmethod
    def rawcandles2list(raw_candles: [{}]) -> [{}]:
        """ Convert candles from raw json to pandas dataframe """
        # Example of raw data {'ch': 'market.BTC-USDT.kline.1min', 'ts': 1686981972955, 'status': 'ok',
        # 'data': [{'id': 1686981240, 'open': 26677.8, 'close': 26663.3, 'high': 26703.9, 'low': 26654.7,
        # 'amount': 33.826, 'vol': 33826, 'trade_turnover': 902606.0032, 'count': 228}], ...
        # Candles to list of dict as a data for df
        if not raw_candles or "data" not in raw_candles:
            return pd.DataFrame()

        ch = raw_candles["ch"]
        ticker = HuobiFeedBase.ticker_of_ch(ch)
        interval = HuobiFeedBase.period_of_ch(ch)

        #dt = datetime.fromtimestamp(raw_candles["ts"] / 1000, tz=timezone.utc)
        # dt = datetime.fromtimestamp(raw_candles["ts"] / 1000)
        # dt = datetime.utcnow()

        # deltatime = pd.Timedelta(interval)
        # times = [dt - deltatime * i for i in range(len(raw_candles["data"]))]
        # data = [HuobiCandlesFeedHbdm.rawcandle2model(time, ticker, interval, raw_candle) for time, raw_candle in
        #         zip(times, raw_candles["data"])]
        data = [HuobiCandlesFeedHbdm.rawcandle2model(ticker, interval, raw_candle) for raw_candle in
                raw_candles["data"]]

        data.sort(key=lambda c: c["close_time"])
        return data

    @staticmethod
    def rawcandle2model(ticker: str, interval: str, raw_candle: {}):
        """ Huobi raw response dictionary to model dictionary """
        # Example of raw candle: {'id': 1686981240, 'open': 26677.8, 'close': 26663.3, 'high': 26703.9, 'low': 26654.7,
        # 'amount': 33.826, 'vol': 33826, 'trade_turnover': 902606.0032, 'count': 228}

        close_time = datetime.fromtimestamp(raw_candle['id'])
        open_time = close_time - pd.Timedelta(interval)

        return {
            "open_time": open_time,
            "close_time": close_time,
            "ticker": ticker,
            "interval": interval,
            "open": raw_candle["open"],
            "high": raw_candle["high"],
            "low": raw_candle["low"],
            "close": raw_candle["close"],
            "vol": raw_candle["vol"]
        }
