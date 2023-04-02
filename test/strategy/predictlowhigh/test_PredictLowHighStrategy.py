import logging
from datetime import datetime
from typing import Optional
from unittest import TestCase

import numpy as np
import pandas as pd

from broker.BinanceBroker import BinanceBroker
from broker.model.Trade import Trade
from strategy.predictlowhigh.PredictLowHighStrategy import PredictLowHighStrategy


class TestPredictLowHighStrategy(TestCase):
    class ModelStub:
        """ Model emulation for unit tests"""

        def predict(self, X, verbose):
            # Emulate some prediction: bid_diff_fut=1, ask_diff_fut=2
            return np.array([1, 2])

    class BrokerStub(BinanceBroker):
        """ Broker emulation, don't trade """

        def __init__(self):
            self._log = logging.getLogger(self.__class__.__name__)
            self.cur_trade: Trade = None

        def create_cur_trade(self, symbol: str, direction: int,
                             quantity: float,
                             price: Optional[float],
                             stop_loss: Optional[float]) -> Optional[Trade]:
            """ Don't trade, just emulate """
            self.cur_trade = Trade(ticker=symbol, side=Trade.order_side_names.get(direction),
                                   open_time=datetime.utcnow(), open_price=price, open_order_id=None,
                                   stop_loss_price=stop_loss, stop_loss_order_id=None,
                                   quantity=quantity)

        def end_cur_trade(self):
            self.cur_trade = None

    class StrategyStub(PredictLowHighStrategy):
        """ Strategy wrapper for tests """

        def __init__(self):
            conf = {"biml.tickers": "test", "biml.strategy.learn.interval.sec": 60, "biml.model.dir": "tmp"}
            super().__init__(None, conf)
            self.profit_loss_ratio = 4
            self.close_profit_loss_ratio = 2
            self.model = TestPredictLowHighStrategy.ModelStub()
            self.broker = TestPredictLowHighStrategy.BrokerStub()

        def save_lastXy(self, X_last: pd.DataFrame, y_pred_last: pd.DataFrame, data_last: pd.DataFrame):
            pass

    def     test_process_new_data__should_set_fut_columns(self):
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

        # Call tested method, strategy should process last bidask record
        strategy.process_new_data()

        self.assertListEqual(strategy.fut_low_high.index.to_pydatetime().tolist(),
                             [datetime.fromisoformat("2023-03-17 15:56:02")])
        self.assertTrue(np.array_equal(strategy.fut_low_high["fut_low"], np.array([6.0]), equal_nan=True))
        self.assertTrue(np.array_equal(strategy.fut_low_high["fut_high"], np.array([7.0]), equal_nan=True))

    def test_predict_low_high__should_predict_last(self):
        # Strategy wrapper
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

        X, y = strategy.predict_low_high()
        self.assertEqual(y.index.to_pydatetime().tolist(), strategy.bid_ask.tail(1).index.to_pydatetime().tolist())

    def test_open_signal_buy(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.StrategyStub()

        actual_signal, actual_loss, actual_profit = strategy.get_open_signal(bid=10, ask=11, fut_high=19, fut_low=9)
        self.assertEqual(actual_signal, 1)
        self.assertEqual(actual_loss, 9)
        self.assertEqual(actual_profit, 19)

    def test_process_new_prediction__should_buy(self):
        strategy = self.StrategyStub()
        strategy.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        strategy.fut_low_high = pd.DataFrame([{"fut_low": 9, "fut_high": 19}])

        # Process: buy or sell or nothing
        open_signal, close_signal = strategy.process_new_prediction()

        self.assertEqual(open_signal, 1)
        self.assertEqual(close_signal, 0)
        self.assertIsNotNone(strategy.broker.cur_trade)
        self.assertEqual(strategy.broker.cur_trade.direction(), 1)

    def test_open_signal_not_buy_low_ratio(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.StrategyStub()

        actual_signal, actual_loss, actual_profit = strategy.get_open_signal(bid=10, ask=11, fut_low=9, fut_high=18.9)

        self.assertEqual(actual_signal, 0)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_open_signal_sell(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.StrategyStub()

        actual_signal, actual_loss, actual_profit = strategy.get_open_signal(bid=10, ask=11, fut_low=2, fut_high=12)

        self.assertEqual(actual_signal, -1)
        self.assertEqual(actual_loss, 12)
        self.assertEqual(actual_profit, 2)

    def test_process_new_prediction__should_sell(self):
        strategy = self.StrategyStub()
        strategy.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        strategy.fut_low_high = pd.DataFrame([{"fut_low": 2, "fut_high": 12}])

        # Process: buy or sell or nothing
        open_signal, close_signal = strategy.process_new_prediction()
        self.assertEqual(open_signal, -1)
        self.assertEqual(close_signal, 0)

        self.assertIsNotNone(strategy.broker.cur_trade)
        self.assertEqual(strategy.broker.cur_trade.direction(), -1)

    def test_open_signal_not_sell_low_ratio(self):
        # Strategy with profit/loss ratio = 4
        strategy = self.StrategyStub()

        actual_signal, actual_loss, actual_profit = strategy.get_open_signal(bid=10, ask=11, fut_high=12, fut_low=2.1)

        self.assertEqual(actual_signal, 0)
        self.assertIsNone(actual_loss)
        self.assertIsNone(actual_profit)

    def test_process_new_prediction__should_close_buy(self):
        strategy = self.StrategyStub()
        strategy.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        strategy.fut_low_high = pd.DataFrame([{"fut_low": 6, "fut_high": 12}])
        strategy.broker.cur_trade = Trade(side="BUY")

        # Process: buy or sell or nothing
        open_signal, close_signal = strategy.process_new_prediction()

        self.assertEqual(open_signal, 0)
        self.assertEqual(close_signal, -1)
        self.assertIsNone(strategy.broker.cur_trade)

    def test_process_new_prediction__should_not_close_buy(self):
        strategy = self.StrategyStub()
        strategy.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        strategy.fut_low_high = pd.DataFrame([{"fut_low": 6.1, "fut_high": 12}])
        strategy.broker.cur_trade = Trade(side="BUY")

        # Process: buy or sell or nothing
        open_signal, close_signal = strategy.process_new_prediction()
        self.assertEqual(open_signal, 0)
        self.assertEqual(close_signal, 0)
        self.assertIsNotNone(strategy.broker.cur_trade)

    def test_process_new_prediction__should_close_sell(self):
        strategy = self.StrategyStub()
        strategy.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        strategy.fut_low_high = pd.DataFrame([{"fut_low": 9, "fut_high": 15}])
        strategy.broker.cur_trade = Trade(side="SELL")

        # Process: buy or sell or nothing
        open_signal, close_signal = strategy.process_new_prediction()

        self.assertEqual(open_signal, 0)
        self.assertEqual(close_signal, 1)
        self.assertIsNone(strategy.broker.cur_trade)

    def test_process_new_prediction__should_not_close_sell(self):
        strategy = self.StrategyStub()
        strategy.bid_ask = pd.DataFrame([{"bid": 10, "ask": 11}])
        strategy.fut_low_high = pd.DataFrame([{"fut_low": 9, "fut_high": 14.9}])
        strategy.broker.cur_trade = Trade(side="SELL")

        # Process: buy or sell or nothing
        open_signal, close_signal = strategy.process_new_prediction()
        self.assertEqual(open_signal, 0)
        self.assertEqual(close_signal, 0)

        self.assertIsNotNone(strategy.broker.cur_trade)
