from datetime import datetime, timedelta
from unittest import TestCase
import pandas as pd
from features.level2.Level2Features import Level2Features


class TestLevel2Features(TestCase):
    def test_expectation(self):
        # Math expectation = sum(price * vol) / sum(vol)
        dt = datetime.fromisoformat('2021-11-26 17:39:00')
        # bid vol = 30, bid_expect = (1*10 + 2*20) / (10+20) = 5/3
        # ask vol = 70, ask expect = (3*30 + 4*40) / (30+40) = (90+160)/70 = 250/70 = 25/7
        # bidask expect = (1*10 + 2*20 + 3*30 + 4*40) / (10+20+30+40) = 300/100 = 3
        level2_data = pd.DataFrame([
            {"datetime": dt, "ask": 4, "ask_vol": 40},
            {"datetime": dt, "ask": 3, "ask_vol": 30},

            {"datetime": dt, "bid": 2, "bid_vol": 20},
            {"datetime": dt, "bid": 1, "bid_vol": 10},

            # Next time moment order book
            {"datetime": dt + timedelta(seconds = 1), "bid": 100, "bid_vol": 10000},

        ])
        expectations = Level2Features().expectation(level2_data)

        self.assertEqual(30, expectations["l2_bid_vol"].tolist()[0])
        self.assertEqual(5/3, expectations["l2_bid_expect"].tolist()[0])

        self.assertEqual(70, expectations["l2_ask_vol"].tolist()[0])
        self.assertEqual(25/7, expectations["l2_ask_expect"].tolist()[0])


        self.assertEqual(3, expectations["l2_bid_ask_expect"].tolist()[0])

        self.assertEqual(2, expectations["l2_bid_max"].tolist()[0])
        self.assertEqual(3, expectations["l2_ask_min"].tolist()[0])
