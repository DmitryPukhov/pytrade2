import os
import sys
from datetime import datetime
from unittest import TestCase
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
from StrategyStub import StrategyStub


class TestPredictLowHighStrategyBasePurge(TestCase):
    def strategy_of(self, window):
        strategy = StrategyStub()
        strategy.history_max_window = window
        return strategy

    def test_purge(self):
        df1 = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "col1": "val1"},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "col2": "val2"},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:12"), "col3": "val3"}
        ]).set_index("datetime", drop=False)

        actual1 = self.strategy_of("10s").purged(df1, "df1")
        self.assertListEqual(["2023-03-17 15:56:02", "2023-03-17 15:56:12"], actual1.index.astype(str).values.tolist())

    def test_purge_emptydf(self):
        actual = self.strategy_of("1s").purged(pd.DataFrame(), "df1")
        self.assertTrue(actual.empty)

    def test_purge_nopurge(self):
        df1 = pd.DataFrame([
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:01"), "col1": "val1"},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:02"), "col2": "val2"},
            {"datetime": datetime.fromisoformat("2023-03-17 15:56:12"), "col3": "val3"}
        ]).set_index("datetime", drop=False)

        actual1= self.strategy_of("11s").purged(df1, "df1")
        self.assertListEqual(["2023-03-17 15:56:01", "2023-03-17 15:56:02", "2023-03-17 15:56:12"],
                             actual1.index.astype(str).values.tolist())

