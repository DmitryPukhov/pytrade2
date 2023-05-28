import logging
from datetime import datetime, timedelta
from typing import Optional
from unittest import TestCase

import numpy as np
import pandas as pd

from broker.BinanceBroker import BinanceBroker
from broker.model.Trade import Trade
from strategy.common.features.PredictLowHighFeatures import PredictLowHighFeatures
from strategy.common.PredictLowHighStrategyBase import PredictLowHighStrategyBase


class TestPredictLowHighStrategyBase(TestCase):
    class ModelStub:
        """ Model emulation for unit tests"""

        def predict(self, X, verbose):
            # Emulate some prediction: bid_min_fut_diff, bit_max_fut_diff, ask_min_fut_diff, ask_max_fut_diff
            return np.array([1, 2, 3, 4])

    class BrokerStub(BinanceBroker):
        """ Broker emulation, don't trade """

        def __init__(self):
            self._log = logging.getLogger(self.__class__.__name__)
            self.cur_trade: Trade = None

        def update_trade_status(self, trade: Trade) -> Trade:
            pass

        def create_cur_trade(self, symbol: str, direction: int,
                             quantity: float,
                             price: Optional[float],
                             stop_loss_price: Optional[float],
                             take_profit_price: Optional[float]) -> Optional[Trade]:
            """ Don't trade, just emulate """
            self.cur_trade = Trade(ticker=symbol, side=Trade.order_side_names.get(direction),
                                   open_time=datetime.utcnow(), open_price=price, open_order_id=None,
                                   stop_loss_price=stop_loss_price, take_profit_price=take_profit_price,
                                   stop_loss_order_id=None,
                                   quantity=quantity)

        def end_cur_trade(self):
            self.cur_trade = None

    class StrategyStub(PredictLowHighStrategyBase):
        """ Strategy wrapper for tests """

        def __init__(self):
            conf = {"pytrade2.tickers": "test", "pytrade2.strategy.learn.interval.sec": 60,
                    "pytrade2.data.dir": "tmp",
                    "pytrade2.strategy.data.gap.max.sec": 10,
                    "pytrade2.strategy.predict.window": "10s",
                    "pytrade2.order.quantity": 0.001}
            super().__init__(None, conf)
            self.profit_loss_ratio = 4
            self.close_profit_loss_ratio = 2
            self.model = TestPredictLowHighStrategyBase.ModelStub()
            self.broker = TestPredictLowHighStrategyBase.BrokerStub()
            self.min_stop_loss = 0
            self.max_stop_loss_coeff = float('inf')

        def save_lastXy(self, X_last: pd.DataFrame, y_pred_last: pd.DataFrame, data_last: pd.DataFrame):
            pass

    def test_is_data_gap(self):
        strategy = self.StrategyStub()
        strategy.data_gap_max = timedelta(seconds=10)

        strategy.bid_ask = pd.DataFrame([{"datetime": datetime.fromisoformat("2023-05-16 21:28:00")}]) \
            .set_index("datetime", drop=False)

        # Level2  11s after bidask, gap> 10s
        strategy.level2 = pd.DataFrame([{"datetime": datetime.fromisoformat("2023-05-16 21:28:11")}]) \
            .set_index("datetime", drop=False)
        self.assertTrue(strategy.is_data_gap())
        self.assertTrue(strategy.is_data_gap)

        # Level2 11s before bidask, gap > 10s
        strategy.level2 = pd.DataFrame([{"datetime": datetime.fromisoformat("2023-05-16 21:27:49")}]) \
            .set_index("datetime", drop=False)
        self.assertTrue(strategy.is_data_gap())
        self.assertTrue(strategy.is_data_gap)

        # No gap
        strategy.level2 = pd.DataFrame([{"datetime": datetime.fromisoformat("2023-05-16 21:28:10")}]) \
            .set_index("datetime", drop=False)
        self.assertFalse(strategy.is_data_gap())

        # Empty level2, bad data quality, but not a gap
        strategy.level2 = pd.DataFrame()
        self.assertFalse(strategy.is_data_gap())

        # Empty bidask, bad data quality, but not a gap
        strategy.level2 = pd.DataFrame([{"datetime": datetime.fromisoformat("2023-05-16 21:28:10")}]) \
            .set_index("datetime", drop=False)
        strategy.bid_ask = pd.DataFrame()
        self.assertFalse(strategy.is_data_gap())

    def test_process_new_data__should_set_fut_columns(self):
        strategy = self.StrategyStub()
        # Set bidask and level2, received by strategy from broker
        strategy.bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8}
        ]).set_index("datetime", drop=False)
        strategy.level2 = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:02'), 'ask': 0.9, 'ask_vol': 1},
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:02'), 'bid': -0.9, 'bid_vol': 1}
        ]).set_index("datetime", drop=False)
        strategy.candles_features = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "doesntmatter": 1}
        ]).set_index("datetime")

        # Init strategy pipeline
        X = PredictLowHighFeatures.features_of(strategy.bid_ask, strategy.level2, strategy.candles_features)
        y = PredictLowHighFeatures.targets_of(strategy.bid_ask, "0s")
        strategy.X_pipe, strategy.y_pipe = strategy.create_pipe(X, y)
        strategy.X_pipe.fit(X)
        strategy.y_pipe.fit(y)

        # Call tested method, strategy should process last bidask record
        strategy.process_new_data()

        self.assertListEqual(strategy.fut_low_high.index.to_pydatetime().tolist(),
                             [datetime.fromisoformat("2023-03-17 15:56:02")])
        # self.assertTrue(np.array_equal(strategy.fut_low_high["bid_min_fut"], np.array([6.0]), equal_nan=True))
        # self.assertTrue(np.array_equal(strategy.fut_low_high["bid_max_fut"], np.array([7.0]), equal_nan=True))

    def test_predict_low_high__should_predict_last(self):
        # Strategy wrapper
        strategy = self.StrategyStub()
        # Set bidask and level2, received by strategy from broker
        strategy.bid_ask = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:12"), "symbol": "asset1",
             "bid": 5, "bid_vol": 6, "ask": 7, "ask_vol": 8}
        ]).set_index("datetime", drop=False)
        strategy.level2 = pd.DataFrame([
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:02'), 'ask': 0.9, 'ask_vol': 1},
            {'datetime': datetime.fromisoformat('2023-03-17 15:56:02'), 'bid': -0.9, 'bid_vol': 1}
        ]).set_index("datetime", drop=False)
        strategy.candles_features = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "doesntmatter": 1}
        ]).set_index("datetime")

        X = PredictLowHighFeatures.features_of(strategy.bid_ask, strategy.level2, strategy.candles_features)
        y = PredictLowHighFeatures.targets_of(strategy.bid_ask)
        strategy.X_pipe, strategy.y_pipe = strategy.create_pipe(X, y)
        strategy.X_pipe.fit(X)
        strategy.y_pipe.fit(y)

        X, y = strategy.predict_low_high()
        self.assertEqual(y.index.to_pydatetime().tolist(), strategy.bid_ask.tail(1).index.to_pydatetime().tolist())

    def test_open_signal_buy(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.StrategyStub()

        actual_signal, price, actual_loss, actual_profit = strategy.get_signal(bid=10, ask=11, bid_max_fut=19,
                                                                               bid_min_fut=9, ask_min_fut=0,
                                                                               ask_max_fut=0)
        self.assertEqual(1, actual_signal)
        self.assertEqual(11, price)
        self.assertEqual(8.5, actual_loss)  # price - sl*1.25
        self.assertEqual(19, actual_profit)

    def test_process_new_prediction__should_buy(self):
        strategy = self.StrategyStub()
        strategy.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        strategy.fut_low_high = pd.DataFrame(
            [{"bid_min_fut": 9, "bid_max_fut": 19, "ask_min_fut": 0, "ask_max_fut": 0}])

        # Process: buy or sell or nothing
        open_signal = strategy.process_new_prediction()

        self.assertEqual(1, open_signal)
        # self.assertEqual(0, close_signal)
        self.assertIsNotNone(strategy.broker.cur_trade)
        self.assertEqual(1, strategy.broker.cur_trade.direction())

    def test_open_signal_not_buy_low_ratio(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.StrategyStub()

        actual_signal, price, actual_loss, actual_profit = strategy.get_signal(bid=10, ask=11, bid_min_fut=9,
                                                                               bid_max_fut=18.9, ask_min_fut=0,
                                                                               ask_max_fut=0)
        self.assertEqual(0, actual_signal)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_open_signal_sell(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.StrategyStub()

        actual_signal, price, actual_loss, actual_profit = strategy.get_signal(bid=10, ask=11, bid_min_fut=0,
                                                                               bid_max_fut=0, ask_min_fut=2,
                                                                               ask_max_fut=12)

        self.assertEqual(-1, actual_signal)
        self.assertEqual(10, price)
        self.assertEqual(12.5, actual_loss)  # adjusted sl*1.25
        self.assertEqual(2, actual_profit)

    def test_process_new_prediction__should_sell(self):
        strategy = self.StrategyStub()
        strategy.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        strategy.fut_low_high = pd.DataFrame(
            [{"bid_min_fut": 0, "bid_max_fut": 0, "ask_min_fut": 2, "ask_max_fut": 12}])

        # Process: buy or sell or nothing
        open_signal = strategy.process_new_prediction()
        self.assertEqual(-1, open_signal)

        self.assertIsNotNone(strategy.broker.cur_trade)
        self.assertEqual(strategy.broker.cur_trade.direction(), -1)

    def test_open_signal_not_sell_low_ratio(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.StrategyStub()

        actual_signal, price, actual_loss, actual_profit = strategy.get_signal(bid=10, ask=11, bid_min_fut=0,
                                                                               bid_max_fut=0, ask_max_fut=12,
                                                                               ask_min_fut=2.1)

        self.assertEqual(0, actual_signal)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)
