from datetime import datetime

from pathlib import Path
from unittest import TestCase

import pandas as pd

from biml.Downloader import Downloader
from biml.feed.BaseFeed import BaseFeed
from biml.feed.LocalFeed import LocalFeed
from biml.feed.LocalWriter import LocalWriter


class TestLocalWriter(TestCase):

    def test__on_candles_should_write_to_daily_files(self):
        # Instance to test
        writer = LocalWriter("data1")

        # New candles1
        dt1 = datetime.fromisoformat("2022-05-06 21:19")
        dt2 = datetime.fromisoformat("2022-05-06 21:20")
        dt3 = datetime.fromisoformat("2022-05-06 21:21")
        dt4 = datetime.fromisoformat("2022-05-06 21:22")
        df = pd.DataFrame(data=[
            [dt1, '38633.76000000', '38806.49000000', '38593.09000000', '38806.49000000',
             '0.15386700',
             dt2, '5956.46023199', 86, '0.11014100', '4264.84945024', '0'],
            [dt3, '38715.21000000', '38768.33000000', '38613.08000000', '38768.33000000',
             '0.10214000',
             dt4, '3953.02369390', 67, '0.04700300', '1820.88901191', '0']],
            columns=BaseFeed.candle_columns)
        candles = {"ticker1": {"interval1": df}}

        # Write candles1
        writer.on_candles(candles)

        # Data csv should be written
        self.assertTrue(Path("data1/ticker1/2022-05-06_ticker1_interval1.csv").exists())

        # New candles2
        dt5 = datetime.fromisoformat("2022-05-06 21:28")
        df = pd.DataFrame(
            [[dt5, '38715.21000000', '38768.33000000', '38613.08000000', '38768.33000000',
              '0.10214000',
              dt5, '3953.02369390', 67, '0.04700300', '1820.88901191', '0']],
            columns=df.columns)
        candles = {"ticker1": {"interval1": df}}

        # Write candles 2
        writer.on_candles(candles)

        # New data should be appended to the same date
        actual_df = pd.read_csv("data1/ticker1/2022-05-06_ticker1_interval1.csv", parse_dates= ["open_time","close_time"], header=0)
        self.assertEqual(actual_df["close_time"].max(), dt5)
        self.assertEqual(actual_df["open_time"].min(), dt1)
        self.assertEqual(len(actual_df), 3)