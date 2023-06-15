import logging
from datetime import datetime

import pandas as pd
from huobi.client.market import MarketClient
from huobi.model.market.candlestick import Candlestick


class HuobiCandlesFeed:
    """
    Huobi candles data feed. Read data from huobi, provide pandas dataframes with that data
    """

    def __init__(self, market_client: MarketClient):
        self._log = logging.getLogger(self.__class__.__name__)
        self.market_client = MarketClient()

    def read_candles(self, ticker, interval, limit):
        """ Read candles from Huobi """

        # Fix interval to huobi format, from 1m to 1min etc.
        if interval.endswith("m"):
            interval += "in"
        self._log.debug(f"Reading {limit} last {ticker} {interval} candles from Huobi")

        # Call Huobi client
        raw_candles = self.market_client.get_candlestick(symbol=ticker.lower(), period=interval, size=limit)
        # Convert raw data to candles df
        candles_df = self.candles2df(ticker=ticker, interval=interval, raw_candles=raw_candles)
        return candles_df

    def candles2df(self, ticker: str, interval: str, raw_candles: [Candlestick]) -> pd.DataFrame:
        """ Convert candles from raw json to pandas dataframe """

        # Candles to list of dict as a data for df
        deltatime = pd.Timedelta(interval)
        times = [datetime.utcnow() - deltatime * i for i in range(len(raw_candles))]
        data = [self.raw_candle2dict(time, ticker, interval, raw_candle) for time, raw_candle in
                zip(times, raw_candles)]

        # Dataframe from dict data
        df = pd.DataFrame(data=data)
        df.set_index("close_time", inplace=True)
        return df[["ticker", "interval", "open", "high", "low", "close", "vol"]].sort_index()

    def raw_candle2dict(self, time, ticker, interval, raw_candle: Candlestick):
        """ Huobi Candlestick object to candle dict"""
        return {
            "close_time": time,
            "ticker": ticker,
            "interval": interval,
            "open": raw_candle.open,
            "high": raw_candle.high,
            "low": raw_candle.low,
            "close": raw_candle.close,
            "vol": raw_candle.vol
        }
