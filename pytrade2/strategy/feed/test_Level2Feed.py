import multiprocessing
from datetime import datetime
from unittest import TestCase

import pandas as pd

from strategy.feed.Level2Feed import Level2Feed


class TestLevel2Feed(TestCase):
    level2_buf = pd.DataFrame([
        datetime.fromisoformat("2023-11-26 00:10"),
        datetime.fromisoformat("2023-11-26 00:10:01"),
        datetime.fromisoformat("2023-11-26 00:11"),
    ], columns=["datetime"])

    def new_strategy(self):
        strategy = Level2Feed({})
        strategy.data_lock = multiprocessing.RLock()

        strategy.level2_buf = self.level2_buf
        return strategy

    def test_on_level2_should_purge_old(self):
        strategy = self.new_strategy()
        strategy.level2_history_period = pd.Timedelta('1min')

        # Call
        strategy.update_level2()

        # Should purge old
        self.assertListEqual(
            [datetime.fromisoformat("2023-11-26 00:10:01"),
             datetime.fromisoformat("2023-11-26 00:11"),
             ],
            strategy.level2["datetime"].tolist())

    def test_on_level2_no_purge(self):
        strategy = self.new_strategy()
        strategy.level2_history_period = pd.Timedelta('2min')

        # Call
        strategy.update_level2()

        self.assertListEqual(
            self.level2_buf["datetime"].tolist(),
            strategy.level2["datetime"].tolist())

    def test_on_level2_purge_all(self):
        strategy = self.new_strategy()
        strategy.level2_history_period = pd.Timedelta('0s')

        # Call
        strategy.update_level2()

        self.assertListEqual(
            [],
            strategy.level2["datetime"].tolist())

    def test_update_level2_should_reset_buf(self):
        strategy = self.new_strategy()
        strategy.level2_history_period = pd.Timedelta('1min')

        # Call
        strategy.update_level2()

        self.assertTrue(strategy.level2_buf.empty)
