import os
import sys
from datetime import datetime
from unittest import TestCase

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
from test_StrategyStub import RegressionStrategyStub
from strategy.features.PredictBidAskFeatures import PredictBidAskFeatures


class TestBidAskRegressionStrategyBase(TestCase):

    def test_predict_low_high__should_predict_last(self):
        # Strategy wrapper
        strategy = RegressionStrategyStub()
        # Set bidask and level2, received by strategy from broker
        strategy.bid_ask_feed.bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:12"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8}
        ]).set_index("datetime", drop=False)
        strategy.level2_feed.level2 = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:02'), 'ask': 0.9, 'ask_vol': 1},
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:02'), 'bid': -0.9, 'bid_vol': 1}
        ]).set_index("datetime", drop=False)

        X = PredictBidAskFeatures.features_of(
            strategy.bid_ask_feed.bid_ask, strategy.level2_feed.level2, strategy.candles_feed.candles_by_interval, strategy.candles_feed.candles_cnt_by_interval,
            past_window="1s")
        y = PredictBidAskFeatures.targets_of(strategy.bid_ask_feed.bid_ask, predict_window="10s")
        strategy.X_pipe, strategy.y_pipe = strategy.create_pipe(X, y)
        strategy.X_pipe.fit(X)
        strategy.y_pipe.fit(y)

        y = strategy.predict(X)
        self.assertEqual(y.index.to_pydatetime().tolist(), strategy.bid_ask_feed.bid_ask.tail(1).index.to_pydatetime().tolist())

    def test_process_new_prediction__should_buy(self):
        strategy = RegressionStrategyStub()
        strategy.bid_ask_feed.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        y_pred = pd.DataFrame(
            [{"bid_min_fut": 9, "bid_max_fut": 19, "ask_min_fut": 11, "ask_max_fut": 11}])

        # Process: buy or sell or nothing
        open_signal = strategy.process_prediction(y_pred)

        self.assertEqual(1, open_signal)
        # self.assertEqual(0, close_signal)
        self.assertIsNotNone(strategy.broker.cur_trade)
        self.assertEqual(1, strategy.broker.cur_trade.direction())

    def test_process_new_prediction__should_sell(self):
        strategy = RegressionStrategyStub()
        strategy.bid_ask_feed.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        y_pred = pd.DataFrame(
            [{"bid_min_fut": 0, "bid_max_fut": 0, "ask_min_fut": 2, "ask_max_fut": 12}])

        # Process: buy or sell or nothing
        open_signal = strategy.process_prediction(y_pred)
        self.assertEqual(-1, open_signal)

        self.assertIsNotNone(strategy.broker.cur_trade)
        self.assertEqual(strategy.broker.cur_trade.direction(), -1)
