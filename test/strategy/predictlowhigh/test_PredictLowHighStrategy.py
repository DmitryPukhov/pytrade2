from datetime import datetime
from unittest import TestCase

import numpy as np
import pandas as pd

from strategy.predictlowhigh.PredictLowHighStrategy import PredictLowHighStrategy


class TestPredictLowHighStrategy(TestCase):
    class ModelStub:
        """ Model emulation for unit tests"""

        def predict(self, X):
            # Emulate some prediction
            return np.array([1, 2])

    def create_strategy_stub(self):
        conf = {"biml.tickers": "test", "biml.strategy.learn.interval.sec": 60, "biml.model.dir": "tmp"}
        strategy = PredictLowHighStrategy(None, config=conf)
        strategy.model = TestPredictLowHighStrategy.ModelStub()
        strategy.save_lastXy = lambda a, b, c: print("save_lastXy stub")
        return strategy

    def test_process_new_data__should_set_fut_columns(self):
        strategy = self.create_strategy_stub()
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
        self.assertTrue(np.array_equal(strategy.fut_low_high["fut_low"], np.array([1.0]), equal_nan=True))
        self.assertTrue(np.array_equal(strategy.fut_low_high["fut_high"], np.array([2.0]), equal_nan=True))

    def test_predict_low_high__should_predict_last(self):
        # Strategy wrapper
        strategy = self.create_strategy_stub()
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
