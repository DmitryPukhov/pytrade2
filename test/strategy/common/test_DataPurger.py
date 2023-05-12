from datetime import datetime
from unittest import TestCase
import pandas as pd
from strategy.common.DataPurger import DataPurger


class TestBidAskFeatures(TestCase):
    def purger_of(self, window):
        dp = DataPurger({})
        dp.purge_window = window
        return dp

    def test_purge(self):
        df1 = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "col1": "val1"},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "col2": "val2"},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:12"), "col3": "val3"}
        ]).set_index("datetime", drop=False)

        actual1 = self.purger_of("10s").purged(df1)
        self.assertListEqual(["2023-03-17 15:56:02", "2023-03-17 15:56:12"], actual1.index.astype(str).values.tolist())

    def test_purge_nowindow(self):
        df1 = pd.DataFrame([{"i1": 1, "col1": "val1"}]).set_index("i1")
        actual1 = self.purger_of(None).purged(df1)
        self.assertListEqual([1], actual1.index.values.tolist())

    def test_purge_emptydf(self):
        actual = self.purger_of("1s").purged(pd.DataFrame())
        self.assertTrue(actual.empty)

    def test_purge_nopurge(self):
        df1 = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "col1": "val1"},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "col2": "val2"},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:12"), "col3": "val3"}
        ]).set_index("datetime", drop=False)

        actual1= self.purger_of("11s").purged(df1)
        self.assertListEqual(["2023-03-17 15:56:01", "2023-03-17 15:56:02", "2023-03-17 15:56:12"],
                             actual1.index.astype(str).values.tolist())

