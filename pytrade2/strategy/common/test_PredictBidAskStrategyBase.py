import os
import sys
from datetime import datetime
from unittest import TestCase

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
from test_StrategyStub import StrategyStub
from strategy.features.PredictBidAskFeatures import PredictBidAskFeatures


class TestPredictBidAskStrategyBase(TestCase):

    def test_predict_low_high__should_predict_last(self):
        # Strategy wrapper
        strategy = StrategyStub()
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

    def test_open_signal_buy(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()

        actual_signal, price, actual_loss, actual_profit, tr_delta = strategy.get_signal(bid=10, ask=11, bid_max_fut=19,
                                                                                         bid_min_fut=9, ask_min_fut=11,
                                                                                         ask_max_fut=11)
        self.assertEqual(1, actual_signal)
        self.assertEqual(11, price)
        self.assertEqual(8.5, actual_loss)  # price - sl*1.25
        self.assertEqual(19, actual_profit)
        self.assertEqual(actual_loss, tr_delta)

    def test_process_new_prediction__should_buy(self):
        strategy = StrategyStub()
        strategy.bid_ask_feed.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        y_pred = pd.DataFrame(
            [{"bid_min_fut": 9, "bid_max_fut": 19, "ask_min_fut": 11, "ask_max_fut": 11}])

        # Process: buy or sell or nothing
        open_signal = strategy.process_prediction(y_pred)

        self.assertEqual(1, open_signal)
        # self.assertEqual(0, close_signal)
        self.assertIsNotNone(strategy.broker.cur_trade)
        self.assertEqual(1, strategy.broker.cur_trade.direction())

    def test_open_signal_should_not_buy_min_profit(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()
        strategy.profit_min_coeff = 0.5

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal(bid=0, ask=100, bid_min_fut=100,
                                                                               bid_max_fut=149, ask_min_fut=100,
                                                                               ask_max_fut=100)
        self.assertEqual(0, actual_signal)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_open_signal_should_buy_gte_min_profit(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()
        strategy.profit_min_coeff = 0.5
        strategy.stop_loss_min_coeff = 0

        actual_signal, price, actual_loss, actual_profit, tp_delta = strategy.get_signal(bid=0, ask=100,
                                                                                         bid_min_fut=100,
                                                                                         bid_max_fut=150,
                                                                                         ask_min_fut=100,
                                                                                         ask_max_fut=100)
        self.assertEqual(1, actual_signal)

    def test_open_signal_buy_should_not_buy_min_loss(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()
        strategy.profit_min_coeff = 0.5
        strategy.stop_loss_min_coeff = 0.1

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal(bid=0, ask=100, bid_min_fut=100,
                                                                                  bid_max_fut=150, ask_min_fut=100,
                                                                                  ask_max_fut=100)
        self.assertEqual(0, actual_signal)

    def test_open_signal_sell(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()

        actual_signal, price, actual_loss, actual_profit, tr_delta = strategy.get_signal(bid=10, ask=11, bid_min_fut=0,
                                                                                         bid_max_fut=0, ask_min_fut=2,
                                                                                         ask_max_fut=12)

        self.assertEqual(-1, actual_signal)
        self.assertEqual(10, price)
        self.assertEqual(12.5, actual_loss)  # adjusted sl*1.25
        self.assertEqual(2, actual_profit)
        self.assertEqual(actual_loss, tr_delta)

    def test_process_new_prediction__should_sell(self):
        strategy = StrategyStub()
        strategy.bid_ask_feed.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        y_pred = pd.DataFrame(
            [{"bid_min_fut": 0, "bid_max_fut": 0, "ask_min_fut": 2, "ask_max_fut": 12}])

        # Process: buy or sell or nothing
        open_signal = strategy.process_prediction(y_pred)
        self.assertEqual(-1, open_signal)

        self.assertIsNotNone(strategy.broker.cur_trade)
        self.assertEqual(strategy.broker.cur_trade.direction(), -1)

    def test_open_signal_not_sell_low_ratio(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal(bid=10, ask=11, bid_min_fut=0,
                                                                               bid_max_fut=0, ask_max_fut=12,
                                                                               ask_min_fut=2.1)

        self.assertEqual(0, actual_signal)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_open_signal_should_not_sell_min_profit(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()
        strategy.profit_min_coeff = 0.5

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal(bid=100, ask=100, bid_min_fut=100,
                                                                               bid_max_fut=149, ask_min_fut=51,
                                                                               ask_max_fut=100)
        self.assertEqual(0, actual_signal)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_open_signal_should_sell_le_min_profit(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()
        strategy.profit_min_coeff = 0.5

        actual_signal, price, actual_loss, actual_profit, tp_delta = strategy.get_signal(bid=100, ask=100,
                                                                                         bid_min_fut=100,
                                                                                         bid_max_fut=100,
                                                                                         ask_min_fut=50,
                                                                                         ask_max_fut=100)
        self.assertEqual(-1, actual_signal)

    def test_open_signal_sell_should_not_sell_min_loss(self):
        # Strategy with profit/loss ratio = 4
        strategy = StrategyStub()
        strategy.profit_min_coeff = 0.5
        strategy.stop_loss_min_coeff = 0.1

        actual_signal, price, actual_loss, actual_profit, _ = strategy.get_signal(bid=100, ask=100, bid_min_fut=100,
                                                                               bid_max_fut=100, ask_min_fut=50,
                                                                               ask_max_fut=100)
        self.assertEqual(0, actual_signal)
        # self.assertEqual(110, actual_loss)
