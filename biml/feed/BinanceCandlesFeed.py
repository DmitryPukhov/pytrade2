import logging

import pandas as pd
from binance.spot import Spot as Client


class BinanceCandlesFeed:
    """
    Binance price data feed. Read data from binance, provide pandas dataframes with that data
    """
    raw_candle_columns = ["open_time", "open", "high", "low", "close", "vol", "close_time", "quote_asset_volume",
                          "number_of_trades", " taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
                          "ignore"]
    candle_columns = ["close_time", "open", "high", "low", "close", "vol"]

    def __init__(self, spot_client: Client):
        self._log = logging.getLogger(self.__class__.__name__)
        self.spot_client: Client = spot_client

    def read_candles(self, ticker, interval, limit):
        """ Read candles from Binance """
        self._log.info(f"Reading {limit} last {ticker} {interval} candles from binance")

        # Call binance client
        raw_candles = self.spot_client.klines(symbol=ticker,
                                              interval=interval,
                                              limit=limit)
        # Convert raw data to candles df
        candles_df = self.candle2df(raw_candles=raw_candles)
        return candles_df

    def candle2df(self, raw_candles) -> pd.DataFrame:
        """ Convert candles from raw json to pandas dataframe """

        # Json to df
        df = pd.DataFrame(data=raw_candles, columns=self.raw_candle_columns)
        df = df[self.candle_columns]

        # Convert time from millis to datetime
        df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')
        df.set_index("close_time", drop=False, inplace=True)

        # Convert strings to float prices
        df[["open", "high", "low", "close", "vol"]] = df[["open", "high", "low", "close", "vol"]].astype(float)

        return df
