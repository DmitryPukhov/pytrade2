import datetime
from unittest import TestCase
import pandas as pd

from strategy.common.LearnDataBalancer import LearnDataBalancer


class LearnDataBalancerTest(TestCase):

    def test_get_balanced_xy__empty(self):
        x, y = LearnDataBalancer.balanced(pd.DataFrame(), pd.DataFrame())
        self.assertTrue(x.empty)
        self.assertTrue(y.empty)

    def test_get_balanced_xy__all_balanced(self):
        x = pd.DataFrame(index=[1, 2, 3], data=[1, 2, 3])
        y = pd.DataFrame(index=[1, 2, 3], data=[-1, 0, 1], columns=['signal'])
        actual_x, actual_y = LearnDataBalancer.balanced(x, y)
        self.assertListEqual([1, 2, 3], actual_x.index.tolist())
        self.assertListEqual([1, 2, 3], actual_y.index.tolist())

    def test_get_balanced_signal_buy_absent(self):
        x = pd.DataFrame(index=[1, 2, 3], data=[1, 2, 3])
        y = pd.DataFrame(index=[1, 2, 3], data=[-1, 0, 0], columns=['signal'])
        actual_x, actual_y = LearnDataBalancer.balanced(x, y)
        self.assertTrue(actual_x.empty)
        self.assertTrue(actual_y.empty)

    def test_get_balanced_signal_sell_absent(self):
        x = pd.DataFrame(index=[1, 2, 3], data=[1, 2, 3])
        y = pd.DataFrame(index=[1, 2, 3], data=[1, 0, 0], columns=['signal'])
        actual_x, actual_y = LearnDataBalancer.balanced(x, y)
        self.assertTrue(actual_x.empty)
        self.assertTrue(actual_y.empty)

    def test_get_balanced_signal_oom_absent(self):
        x = pd.DataFrame(index=[1, 2, 3], data=[1, 2, 3])
        y = pd.DataFrame(index=[1, 2, 3], data=[1, -1, 1], columns=['signal'])
        actual_x, actual_y = LearnDataBalancer.balanced(x, y)
        self.assertTrue(actual_x.empty)
        self.assertTrue(actual_y.empty)

    def test_get_balanced_xy__keep_last_signal(self):
        x = pd.DataFrame(index=[1, 2, 3, 4], data=[1, 2, 3, 4])
        y = pd.DataFrame(index=[1, 2, 3, 4], data=[-1, 0, 1, 1], columns=['signal'])
        actual_x, actual_y = LearnDataBalancer.balanced(x, y)
        # Should keep last index4, drop index 3
        self.assertListEqual([1, 2, 4], actual_x.index.tolist())
        self.assertListEqual([1, 2, 4], actual_y.index.tolist())

