from datetime import datetime
from unittest import TestCase

from feed.history.CandlesExchDownloader import CandlesExchDownloader


class TestCandlesDownloader(TestCase):

    def test_intervals_0days(self):
        intervals = CandlesExchDownloader.date_intervals(datetime.fromisoformat("2023-12-18"),
                                                         datetime.fromisoformat("2023-12-18"))
        self.assertListEqual([], intervals)

    def test_intervals_1day(self):
        intervals = CandlesExchDownloader.date_intervals(datetime.fromisoformat("2023-12-18"),
                                                         datetime.fromisoformat("2023-12-19"))
        self.assertListEqual(
            [(datetime.fromisoformat("2023-12-18 00:01:00"), datetime.fromisoformat("2023-12-19 00:00:00"))],
            intervals)

    def test_intervals_2days(self):
        intervals = CandlesExchDownloader.date_intervals(datetime.fromisoformat("2023-12-17"),
                                                         datetime.fromisoformat("2023-12-19"))
        self.assertListEqual(
            [(datetime.fromisoformat("2023-12-17 00:01:00"), datetime.fromisoformat("2023-12-18 00:00:00")),
                (datetime.fromisoformat("2023-12-18 00:01:00"), datetime.fromisoformat("2023-12-19 00:00:00")),
             ],
            intervals)

    def test_last_days_2(self):
        actual = list(CandlesExchDownloader.last_days(datetime.fromisoformat("2023-12-18"), 2, '1min'))

        self.assertListEqual(
            [(datetime.fromisoformat("2023-12-18 00:01:00"), datetime.fromisoformat("2023-12-19 00:00:00")),
             (datetime.fromisoformat("2023-12-17 00:01:00"), datetime.fromisoformat("2023-12-18 00:00:00")),
             ], actual)

    def test_last_days_1(self):
        actual = list(CandlesExchDownloader.last_days(datetime.fromisoformat("2023-12-18"), 1, '1min'))
        self.assertListEqual(
            [(datetime.fromisoformat("2023-12-18 00:01:00"), datetime.fromisoformat("2023-12-19 00:00:00"))], actual)

    def test_last_days_0(self):
        actual = list(CandlesExchDownloader.last_days(datetime.fromisoformat("2023-12-18"), 0, '1min'))
        self.assertListEqual(
            [], actual)
