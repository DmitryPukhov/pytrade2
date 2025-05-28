from unittest import TestCase

import pandas as pd

from feed.history.Preprocessor import Preprocessor


class TestPreprocessor(TestCase):
    def test_transform_should_resample_level2_to_1min(self):
        df = pd.to_datetime("2025-05-28 00:01:03")
        input = pd.DataFrame([
            # 00:00:00 - 00:00:59 ticks
            {"datetime": pd.to_datetime("2025-05-28 00:00:00"), "bid": 1, "ask": 1, "bid_vol": 1, "ask_vol": 2},
            {"datetime": pd.to_datetime("2025-05-28 00:00:01"), "bid": 2, "ask": 1, "bid_vol": 2, "ask_vol": 2},
            # 00:01:00 - 00:01:59 ticks
            {"datetime": pd.to_datetime("2025-05-28 00:01:01"), "bid": 3, "ask": 1, "bid_vol": 1, "ask_vol": 2},
            {"datetime": pd.to_datetime("2025-05-28 00:01:02"), "bid": 4, "ask": 1, "bid_vol": 1, "ask_vol": 2},
            {"datetime": pd.to_datetime("2025-05-28 00:01:03"), "bid": 5, "ask": 1, "bid_vol": 1, "ask_vol": 2},

        ]).set_index("datetime", drop=False)
        print(input.resample("1min", label="right", closed="left").agg("mean"))
        preprocessed = Preprocessor(None).transform(input, "level2")
        self.assertEqual(2, len(preprocessed))

    def test_transform_should_resample_candles_to_1min(self):
        base_candle = {"open_time": pd.to_datetime("2025-05-28 00:00:00"), "open": 1, "high": 1, "low": 1, "close": 2, "vol": 1}
        input = pd.DataFrame([
            # 00:00:00 - 00:00:59 ticks
            {**{"close_time": pd.to_datetime("2025-05-28 00:00:00")}, **base_candle},
            {**{"close_time": pd.to_datetime("2025-05-28 00:00:01")}, **base_candle},
            # 00:01:00 - 00:01:59 ticks
            {**{"close_time": pd.to_datetime("2025-05-28 00:01:01")}, **base_candle},
            {**{"close_time": pd.to_datetime("2025-05-28 00:01:02")}, **base_candle},
            {**{"close_time": pd.to_datetime("2025-05-28 00:01:03")}, **base_candle}
        ]).set_index("close_time", drop=False)
        preprocessed = Preprocessor(None).transform(input, "candles")
        self.assertEqual(2, len(preprocessed))
