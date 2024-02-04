import multiprocessing
from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock

import pandas as pd

from strategy.feed.Level2Feed import Level2Feed


class TestLevel2Feed(TestCase):
    level2_buf = pd.DataFrame([
        datetime.fromisoformat("2023-11-26 00:10"),
        datetime.fromisoformat("2023-11-26 00:10:01"),
        datetime.fromisoformat("2023-11-26 00:11"),
    ], columns=["datetime"])

    def new_level2_feed(self):
        level2_feed = Level2Feed({"pytrade2.exchange": "exchange1"}, MagicMock(), multiprocessing.RLock(), multiprocessing.Event())
        level2_feed.data_lock = multiprocessing.RLock()

        level2_feed.level2_buf = self.level2_buf
        return level2_feed

    def test_on_level2_should_purge_old(self):
        strategy = self.new_level2_feed()
        strategy.history_max_window = pd.Timedelta('1min')

        # Call
        strategy.apply_buf()

        # Should purge old
        self.assertListEqual(
            [datetime.fromisoformat("2023-11-26 00:10:01"),
             datetime.fromisoformat("2023-11-26 00:11"),
             ],
            strategy.level2["datetime"].tolist())

    def test_on_level2_no_purge(self):
        level2_feed = self.new_level2_feed()
        level2_feed.history_max_window = pd.Timedelta('2min')

        # Call
        level2_feed.apply_buf()

        self.assertListEqual(
            self.level2_buf["datetime"].tolist(),
            level2_feed.level2["datetime"].tolist())

    def test_on_level2_purge_all(self):
        level2_feed = self.new_level2_feed()
        level2_feed.history_max_window = pd.Timedelta('0s')

        # Call
        level2_feed.apply_buf()

        self.assertListEqual(
            [],
            level2_feed.level2["datetime"].tolist())

    def test_update_level2_should_reset_buf(self):
        level2_feed = self.new_level2_feed()
        level2_feed.history_max_window = pd.Timedelta('1min')

        # Call
        level2_feed.apply_buf()

        self.assertTrue(level2_feed.level2_buf.empty)
