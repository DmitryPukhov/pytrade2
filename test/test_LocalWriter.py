import os
from datetime import datetime
from pathlib import Path
from unittest import TestCase
import shutil
import pandas as pd

from biml.feed.BaseFeed import BaseFeed
from biml.feed.LocalWriter import LocalWriter


class TestLocalWriter(TestCase):
    data_dir = "data1"

    def setUp(self):
        self.cleanup()

    def tearDown(self):
        self.cleanup()

    @staticmethod
    def cleanup():
        if os.path.exists(TestLocalWriter.data_dir):
            shutil.rmtree(TestLocalWriter.data_dir)

    def test__double_write_should_write_once(self):
        writer = LocalWriter("data1")
        close_dt = datetime.fromisoformat("2022-05-06 22:29")
        candles = pd.DataFrame(data=[
            # 6th of May
            [datetime.fromisoformat("2022-05-06 21:19"), 1, 2, 3, 4, 5,
             close_dt, 6, 7, 8, 9, 10]
        ], columns=BaseFeed.candle_columns)
        writer.on_candles("ticker1", "interval1", candles)

        self.assertEqual(writer.last_time_dict, {"ticker1": {"interval1": close_dt}})
        self.assertEqual(writer.get_last_data_time("ticker1", "interval1"), close_dt)

    def test__get_last_data_time_empty_data(self):
        writer = LocalWriter(TestLocalWriter.data_dir)
        self.assertEqual(writer.get_last_data_time("ticker1", "interval1"), pd.Timestamp.min)

    def test__get_last_data_time(self):
        writer = LocalWriter("data1")

        df_interval1 = pd.DataFrame(data=[
            # 6th of May
            [datetime.fromisoformat("2022-05-06 21:19"), 1, 2, 3, 4, 5,
             datetime.fromisoformat("2022-05-06 21:29"), 6, 7, 8, 9, 10],
            # 7th of May
            [datetime.fromisoformat("2022-05-07 21:29"), 1, 2, 3, 4, 5,
             datetime.fromisoformat("2022-05-07 21:29"), 6, 7, 8, 9, 10],
            [datetime.fromisoformat("2022-05-07 21:19"), 1, 2, 3, 4, 5,
             datetime.fromisoformat("2022-05-07 21:31"), 6, 7, 8, 9, 10],
        ], columns=BaseFeed.candle_columns)
        df_interval2 = pd.DataFrame(data=[
            # 6th of May
            [datetime.fromisoformat("2022-05-06 21:19"), 1, 2, 3, 4, 5,
             datetime.fromisoformat("2022-05-06 21:29"), 6, 7, 8, 9, 10],
            # 7th of May
            [datetime.fromisoformat("2022-05-07 21:29"), 1, 2, 3, 4, 5,
             datetime.fromisoformat("2022-05-07 21:30"), 6, 7, 8, 9, 10],
        ], columns=BaseFeed.candle_columns)

        writer.on_candles("ticker1", "interval1", df_interval1)
        writer.on_candles("ticker1", "interval1", df_interval1)

        self.assertEqual(writer.get_last_data_time("ticker1", "interval1"),
                         datetime.fromisoformat("2022-05-07 21:31"))

    def test__on_candles_should_write_to_daily_files(self):
        # Instance to test
        writer = LocalWriter("data1")

        # New candles1
        dt1 = datetime.fromisoformat("2022-05-06 21:19")
        dt2 = datetime.fromisoformat("2022-05-06 21:20")
        dt3 = datetime.fromisoformat("2022-05-06 21:21")
        dt4 = datetime.fromisoformat("2022-05-06 21:22")
        candles = pd.DataFrame(data=[
            [dt1, '38633.76000000', '38806.49000000', '38593.09000000', '38806.49000000',
             '0.15386700',
             dt2, '5956.46023199', 86, '0.11014100', '4264.84945024', '0'],
            [dt3, '38715.21000000', '38768.33000000', '38613.08000000', '38768.33000000',
             '0.10214000',
             dt4, '3953.02369390', 67, '0.04700300', '1820.88901191', '0']],
            columns=BaseFeed.candle_columns)

        # Write candles1
        writer.on_candles("ticker1", "interval1", candles)

        # Data csv should be written
        self.assertTrue(Path("data1/ticker1/2022-05-06_ticker1_interval1.csv").exists())

        # New candles2
        dt5 = datetime.fromisoformat("2022-05-06 21:28")
        candles = pd.DataFrame(
            [[dt5, '38715.21000000', '38768.33000000', '38613.08000000', '38768.33000000',
              '0.10214000',
              dt5, '3953.02369390', 67, '0.04700300', '1820.88901191', '0']],
            columns=candles.columns)

        # Write candles 2
        writer.on_candles("ticker1", "interval1", candles)

        # New data should be appended to the same date
        actual_df = pd.read_csv("data1/ticker1/2022-05-06_ticker1_interval1.csv",
                                parse_dates=["open_time", "close_time"], header=0)
        self.assertEqual(actual_df["close_time"].max(), dt5)
        self.assertEqual(actual_df["open_time"].min(), dt1)
        self.assertEqual(len(actual_df), 3)
