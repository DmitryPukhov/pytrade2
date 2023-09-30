from unittest import TestCase
import pandas as pd

from strategy.common.LearnDataBalancer import LearnDataBalancer


class LearnDataBalancerTest(TestCase):

    @staticmethod
    def balancer_with_data():
        balancer = LearnDataBalancer()
        balancer.x_dict = {
            -1: pd.DataFrame([1, 2, 3, 4]),
            0: pd.DataFrame([1, 2, 3]),
            1: pd.DataFrame([1, 2, 3])
        }
        balancer.y_dict = {
            -1: pd.DataFrame([1, 2, 3, 4]),
            0: pd.DataFrame([1, 2, 3]),
            1: pd.DataFrame([1, 2, 3])
        }
        return balancer

    def test_balance_empty_df(self):
        balancer = LearnDataBalancer()
        balancer.balance()
        for signal in (-1, 0, 1):
            self.assertTrue(balancer.x_dict[signal].empty)
            self.assertTrue(balancer.y_dict[signal].empty)

    def test_balance_zero_max_len(self):
        balancer = self.balancer_with_data()
        balancer.max_len = 0
        balancer.balance()
        for signal in (-1, 0, 1):
            self.assertTrue(balancer.x_dict[signal].empty)
            self.assertTrue(balancer.y_dict[signal].empty)

    def test_balance(self):
        # Prepare unbalanced data
        balancer = self.balancer_with_data()
        # Call
        balancer.balance()

        # Assert max item is balanced
        self.assertEqual([3, 3, 3], [len(df) for df in balancer.x_dict.values()])
        self.assertEqual([3, 3, 3], [len(df) for df in balancer.y_dict.values()])

        self.assertListEqual([[2], [3], [4]], balancer.x_dict[-1].values.tolist())
        self.assertListEqual([[2], [3], [4]], balancer.y_dict[-1].values.tolist())
