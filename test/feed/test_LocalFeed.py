from datetime import datetime
from unittest import TestCase

from feed.LocalFeed import LocalFeed


class TestLocalFeed(TestCase):
    def test_is_between_daily(self):
        # Between
        self.assertTrue(LocalFeed.is_between("dir/2012-05-01_ticker1_interval1.csv", datetime.fromisoformat("2012-04-30"), datetime.fromisoformat("2012-05-02")))
        # Starts on low bound
        self.assertTrue(LocalFeed.is_between("dir/2012-05-01_ticker1_interval1.csv", datetime.fromisoformat("2012-05-01"), datetime.fromisoformat("2012-05-02")))
        # High bound = low bound, but high bound is exclusive
        self.assertFalse(LocalFeed.is_between("dir/2012-05-01_ticker1_interval1.csv", datetime.fromisoformat("2012-05-01"), datetime.fromisoformat("2012-05-01")))

        # Out of bounds
        self.assertFalse(LocalFeed.is_between("dir/2012-04-29_ticker1_interval1.csv", datetime.fromisoformat("2012-05-01"), datetime.fromisoformat("2012-04-30")))
        self.assertFalse(LocalFeed.is_between("dir/2012-05-01_ticker1_interval1.csv", datetime.fromisoformat("2012-05-02"), datetime.fromisoformat("2012-05-04")))

    def test_is_between_yearly(self):
        # Between
        self.assertTrue(LocalFeed.is_between("dir/2012_ticker1_interval1.csv", datetime.fromisoformat("2011-04-30"), datetime.fromisoformat("2013-01-01")))
        # Starts on low bound
        self.assertTrue(LocalFeed.is_between("dir/2012_ticker1_interval1.csv", datetime.fromisoformat("2012-05-01"), datetime.fromisoformat("2013-01-01")))
        # High bound = low bound, file could contain some data inside low-high interval
        self.assertFalse(LocalFeed.is_between("dir/2012_ticker1_interval1.csv", datetime.fromisoformat("2012-05-01"), datetime.fromisoformat("2012-01-01")))

        # Out of bounds
        self.assertFalse(LocalFeed.is_between("dir/2012_ticker1_interval1.csv", datetime.fromisoformat("2010-01-01"), datetime.fromisoformat("2011-01-01")))
        self.assertFalse(LocalFeed.is_between("dir/2012_ticker1_interval1.csv", datetime.fromisoformat("2013-01-01"), datetime.fromisoformat("2014-01-01")))
