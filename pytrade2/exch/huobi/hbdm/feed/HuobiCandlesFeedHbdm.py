import logging
from datetime import datetime

import pandas as pd

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.feed.HuobiFeedBase import HuobiFeedBase


class HuobiCandlesFeedHbdm(HuobiFeedBase):
    """
    Huobi candles feed on Huobi derivatives market.
    """

    def __init__(self, config: dict, rest_client: HuobiRestClient):
        self._log = logging.getLogger(self.__class__.__name__)
        self.rest_client = rest_client

    def read_candles(self, ticker, interval, limit):
        """ Read candles from Huobi """

        # Get candles from huobi rest
        path = "/linear-swap-ex/market/history/kline"
        params = {"contract_code": ticker,
                  "period": interval,
                  "size": limit}
        res = self.rest_client.get(path, params)

        # Convert
        df = self.rawcandles2df(res)
        return df

    @staticmethod
    def rawcandles2df(raw_candles: [{}]) -> pd.DataFrame:
        """ Convert candles from raw json to pandas dataframe """
        # Example of raw data {'ch': 'market.BTC-USDT.kline.1min', 'ts': 1686981972955, 'status': 'ok',
        # 'data': [{'id': 1686981240, 'open': 26677.8, 'close': 26663.3, 'high': 26703.9, 'low': 26654.7,
        # 'amount': 33.826, 'vol': 33826, 'trade_turnover': 902606.0032, 'count': 228}], ...
        # Candles to list of dict as a data for df
        if not raw_candles or "data" not in raw_candles:
            return pd.DataFrame()

        ch = raw_candles["ch"]
        ticker = HuobiFeedBase.ticker_of_ch(ch)
        interval = HuobiFeedBase.interval_of_ch(ch)

        dt = datetime.utcfromtimestamp(raw_candles["ts"] / 1000)

        deltatime = pd.Timedelta(interval)
        times = [dt - deltatime * i for i in range(len(raw_candles["data"]))]
        data = [HuobiCandlesFeedHbdm.rawcandle2model(time, ticker, interval, raw_candle) for time, raw_candle in
                zip(times, raw_candles["data"])]

        # Dataframe from dict data
        df = pd.DataFrame(data=data)
        df.set_index("close_time", inplace=True)
        return df[["ticker", "interval", "open", "high", "low", "close", "vol"]].sort_index()

    @staticmethod
    def rawcandle2model(time: datetime, ticker: str, interval: {}, raw_candle: {}):
        """ Huobi raw responce dictionary to model dictionary """
        # Example of raw candle: {'id': 1686981240, 'open': 26677.8, 'close': 26663.3, 'high': 26703.9, 'low': 26654.7,
        # 'amount': 33.826, 'vol': 33826, 'trade_turnover': 902606.0032, 'count': 228}
        return {
            "close_time": time,
            "ticker": ticker,
            "interval": interval,
            "open": raw_candle["open"],
            "high": raw_candle["high"],
            "low": raw_candle["low"],
            "close": raw_candle["close"],
            "vol": raw_candle["vol"]
        }
