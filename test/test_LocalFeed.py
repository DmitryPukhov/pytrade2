from pathlib import Path
from unittest import TestCase

import pandas as pd

from biml.feed.LocalFeed import LocalFeed


class TestLocalFeed(TestCase):

    def test__get_file_name(self):
        feed = LocalFeed(ticker="ticker1", data_dir="folder1", candle_fast_interval="", candle_fast_limit=0,
                         candle_medium_interval="", candle_medium_limit=0)
        self.assertEqual(feed.get_file_name("interval1", "time1"), "folder1/ticker1/ticker1_interval1_time1.csv")

    def test__extract_time(self):
        feed = LocalFeed(ticker="", data_dir="", candle_fast_interval="", candle_fast_limit=0,
                         candle_medium_interval="", candle_medium_limit=0)
        self.assertEqual(feed.extract_time("folder1/ticker1_interval1_time1.csv"), "time1")
        self.assertEqual(feed.extract_time("ticker1_interval1_time1.csv"), "time1")
        self.assertEqual(feed.extract_time("ticker1_interval1_time1.json"), "time1")
        self.assertEqual(feed.extract_time("ticker1_interval1_time1"), "time1")
        self.assertIsNone(feed.extract_time(None))

    def test__write_new(self):
        feed = LocalFeed(ticker="ticker1", data_dir="data1", candle_fast_interval="", candle_fast_limit=0,
                         candle_medium_interval="", candle_medium_limit=0)
        df = pd.DataFrame(data=[
            [1000000000001, '38633.76000000', '38806.49000000', '38593.09000000', '38806.49000000', '0.15386700',
             1000000000002, '5956.46023199', 86, '0.11014100', '4264.84945024', '0'],
            [1000000000003, '38715.21000000', '38768.33000000', '38613.08000000', '38768.33000000', '0.10214000',
             1000000000004, '3953.02369390', 67, '0.04700300', '1820.88901191', '0']], columns=feed.candle_columns)

        # Write from scratch
        feed.write_new(interval="interval1", all_data=df)

        # Data csv should be written
        self.assertTrue(Path("data1/ticker1/ticker1_interval1_1000000000004.csv").exists())

        # Append new data to df and write new
        df = df.append(
            pd.DataFrame(
                [[1000000000005, '38715.21000000', '38768.33000000', '38613.08000000', '38768.33000000', '0.10214000',
                  1000000000006, '3953.02369390', 67, '0.04700300', '1820.88901191', '0']], columns=df.columns))
        feed.write_new(interval="interval1", all_data=df)

        # New data should be written, file name contain last time
        self.assertTrue(Path("data1/ticker1/ticker1_interval1_1000000000006.csv").exists())

        new_df = pd.read_csv("data1/ticker1/ticker1_interval1_1000000000006.csv", header=0)

        self.assertEqual(new_df["close_time"].max(), 1000000000006)
        self.assertEqual(len(new_df), 1)
