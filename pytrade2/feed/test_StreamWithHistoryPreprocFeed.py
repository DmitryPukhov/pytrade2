from collections import defaultdict
from unittest import TestCase
from unittest.mock import MagicMock

import pandas as pd

from feed.StreamWithHistoryPreprocFeed import StreamWithHistoryPreprocFeed


class TestStreamHistoryPreprocFeed(TestCase):
    @staticmethod
    def new_feed():
        config = defaultdict(str)
        config["pytrade2.feed.candles.periods"] = "1min,5min"
        config["pytrade2.feed.candles.counts"] = "1,1"
        config["pytrade2.feed.candles.history.days"] = "1"
        feed = StreamWithHistoryPreprocFeed(config, MagicMock())
        feed.kind = "level2"
        return feed

    def test_preproc_incremental_all_empty(self):
        feed = self.new_feed()
        feed.preproc_data_df = pd.DataFrame()

        # Call
        preprocessed_df = feed.preproc_incremental(pd.DataFrame())

        self.assertTrue(preprocessed_df.empty)
        self.assertTrue(feed.preproc_data_df.empty)

    def test_preproc_incremental_if_new_data_is_empty_should_return_old_data(self):
        feed = self.new_feed()
        feed.preproc_data_df = pd.DataFrame([
            {"datetime": pd.to_datetime("2025-05-28 13:22:00"), "value": 1}
        ]).set_index("datetime", drop=False)

        # Call
        preprocessed = feed.preproc_incremental(pd.DataFrame())

        self.assertListEqual([pd.to_datetime("2025-05-28 13:22:00")], preprocessed.index.tolist())
        self.assertListEqual(preprocessed.index.tolist(), feed.preproc_data_df.index.tolist())

    def test_preproc_incremental_if_old_data_is_empty_should_return_empty(self):
        # If old data is empty, history is not loaded yet, no base to incremental preprocessing

        feed = self.new_feed()
        new_df = pd.DataFrame([
            {"datetime": pd.to_datetime("2025-05-28 13:22:00"), "value": 1}
        ]).set_index("datetime", drop=False)

        # Call
        preprocessed_df = feed.preproc_incremental(new_df)

        self.assertTrue(preprocessed_df.empty)
        self.assertTrue(feed.preproc_data_df.empty)

    def test_preproc_incremental_should_concatenate(self):
        feed = self.new_feed()
        base_fields = {"bid": 0, "ask": 0, "bid_vol": 0, "ask_vol": 0}
        feed.preproc_data_df = pd.DataFrame([
            {**{"datetime": pd.to_datetime("2025-05-28 00:00:00")}, **base_fields},
        ]).set_index("datetime", drop=False)
        # 00:00:01 - 00:01:00 minute
        new_raw_data_df = pd.DataFrame([
            {**{"datetime": pd.to_datetime("2025-05-28 00:00:01"), "value": 1}, **base_fields},
            {**{"datetime": pd.to_datetime("2025-05-28 00:01:00"), "value": 2}, **base_fields},
        ]).set_index("datetime", drop=False)

        # Call
        preprocessed = feed.preproc_incremental(new_raw_data_df)

        self.assertListEqual([pd.to_datetime("2025-05-28 00:00:00"),
                              pd.to_datetime("2025-05-28 00:01:00")], preprocessed.index.tolist())

    def test_preproc_incremental_if_new_before_old_should_skip(self):
        feed = self.new_feed()
        base_fields = {"bid": 0, "ask": 0, "bid_vol": 0, "ask_vol": 0}

        # Old data
        feed.preproc_data_df = pd.DataFrame([
            {**{"datetime": pd.to_datetime("2025-05-28 00:02:00")}, **base_fields},
        ]).set_index("datetime", drop=False)
        # New data is not after old data
        new_raw_data_df = pd.DataFrame([
            {**{"datetime": pd.to_datetime("2025-05-28 00:01:00"), "value": 1}, **base_fields},
            {**{"datetime": pd.to_datetime("2025-05-28 00:02:00"), "value": 1}, **base_fields},
        ]).set_index("datetime", drop=False)

        # Call
        preprocessed = feed.preproc_incremental(new_raw_data_df)

        self.assertListEqual([pd.to_datetime("2025-05-28 00:02:00"),
                              ], preprocessed.index.tolist())

    def test_preproc_incremental_if_new_after_old_should_append(self):
        feed = self.new_feed()
        base_fields = {"bid": 0, "ask": 0, "bid_vol": 0, "ask_vol": 0}

        # Old data
        feed.preproc_data_df = pd.DataFrame([
            {**{"datetime": pd.to_datetime("2025-05-28 00:00:00")}, **base_fields},
        ]).set_index("datetime", drop=False)
        # New data is not after old data
        new_raw_data_df = pd.DataFrame([
            {**{"datetime": pd.to_datetime("2025-05-28 00:01:00"), "value": 1}, **base_fields},
            {**{"datetime": pd.to_datetime("2025-05-28 00:02:00"), "value": 1}, **base_fields},
        ]).set_index("datetime", drop=False)

        # Call
        preprocessed = feed.preproc_incremental(new_raw_data_df)

        self.assertListEqual([
            pd.to_datetime("2025-05-28 00:00:00"),
            pd.to_datetime("2025-05-28 00:01:00"),
            pd.to_datetime("2025-05-28 00:02:00"),
        ], preprocessed.index.tolist())
