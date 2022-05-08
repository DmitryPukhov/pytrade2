from typing import List, Dict

import pandas as pd


class TickerInfo:
    """
    Info about ticker feed: name, intervals (M1, M15 etc), limits for candles
    """

    def __init__(self, ticker, candle_intervals: List, candle_limits: List):
        self.ticker = ticker
        # {interval: candle count to ask binance for}
        self.candle_limits = dict(zip(candle_intervals, candle_limits))
        self.candle_intervals = candle_intervals
        # {interval: last time}
        self.candle_last_times: Dict[str, pd.Timestamp] = dict(
            [(interval, None) for interval in candle_intervals])

