from collections import defaultdict
from unittest import TestCase
from unittest.mock import MagicMock, patch
import pandas as pd
from pytrade2.strategy.SignalClassificationFeaturesStrategy import SignalClassificationFeaturesStrategy


class TestSignalClassificationFeaturesStrategy(TestCase):

    def setUp(self):
        # Patch metrics
        self.metrics_patch = patch(
            "pytrade2.metrics.MetricServer.MetricServer.metrics",
            return_value=MagicMock()
        )
        self.mock_metrics = self.metrics_patch.start()
        super().setUpClass()

    def tearDown(self):
        # Unpatch metrics
        self.metrics_patch.stop()
        super().tearDownClass()

    @staticmethod
    def _new_strategy() -> SignalClassificationFeaturesStrategy:
        config = defaultdict(str)
        config["pytrade2.strategy.stoploss.coeff"] = 0.0
        config["pytrade2.strategy.profit.loss.ratio"] = 0.0

        strategy = SignalClassificationFeaturesStrategy(config, MagicMock())
        strategy.model = MagicMock()
        strategy.broker = MagicMock()
        strategy.broker.cur_trade = None

        strategy.risk_manager = MagicMock()
        strategy.bid_ask_feed = MagicMock()
        return strategy

    def test_strategy_caught_new_data_event(self):
        feature = {"datetime": "2025-06-20 11:54:00", "value": 1}
        strategy = self._new_strategy()
        self.assertFalse(strategy.new_data_event.is_set())
        strategy._features_feed._on_message(feature)
        self.assertTrue(strategy.new_data_event.is_set())

    def test_process_new_data_oom(self):
        feature = {"datetime": "2025-06-20 11:54:00", "value": 1}
        strategy = self._new_strategy()

        # Out of market signal predicted
        strategy.predict = MagicMock(return_value=pd.DataFrame([0], columns=["signal"]))
        strategy._features_feed._on_message(feature)
        strategy.process_new_data()
        # OOM, no broker call
        strategy.broker.create_cur_trade.assert_not_called()

    def test_process_new_data_buy(self):
        feature = {"datetime": "2025-06-20 11:54:00", "value": 1}
        strategy = self._new_strategy()

        # Out of market signal predicted
        strategy.predict = MagicMock(return_value=pd.DataFrame([1], columns=["signal"]))
        strategy.bid_ask_feed.data = pd.DataFrame([{"bid": 99.0, "ask": 101.0}])

        strategy._features_feed._on_message(feature)
        strategy.process_new_data()
        # OOM, no broker call
        calls = strategy.broker.create_cur_trade.call_args_list
        assert len(calls) == 1
        assert calls[0].kwargs["direction"] == 1
        assert calls[0].kwargs["price"] == 101

    def test_process_new_data_sell(self):
        feature = {"datetime": "2025-06-20 11:54:00", "value": 1}
        strategy = self._new_strategy()

        # Out of market signal predicted
        strategy.predict = MagicMock(return_value=pd.DataFrame([-1], columns=["signal"]))
        strategy.bid_ask_feed.data = pd.DataFrame([{"bid": 99.0, "ask": 101.0}])

        strategy._features_feed._on_message(feature)
        strategy.process_new_data()
        # OOM, no broker call
        calls = strategy.broker.create_cur_trade.call_args_list
        assert len(calls) == 1
        assert calls[0].kwargs["direction"] == -1
        assert calls[0].kwargs["price"] == 99





