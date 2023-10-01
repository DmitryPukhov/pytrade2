import datetime
from unittest import TestCase
import pandas as pd

from strategy.common.LearnDataBalancer import LearnDataBalancer


class LearnDataBalancerTest(TestCase):

    @staticmethod
    def balancer_with_data():
        balancer = LearnDataBalancer()
        balancer.x_dict = {
            -1: pd.DataFrame(data=[1, 2, 3, 4], index=[1, 2, 3, 4]),
            0: pd.DataFrame(data=[5, 6, 7], index=[5, 6, 7]),
            1: pd.DataFrame([8, 10, 9], index=[8, 10, 9])
        }
        balancer.y_dict = {
            -1: pd.DataFrame(data=[10, 20, 30, 40], index=[1, 2, 3, 4]),
            0: pd.DataFrame(data=[50, 60, 70], index=[5, 6, 7]),
            1: pd.DataFrame(data=[80, 100, 90], index=[8, 10, 9])
        }
        return balancer

    def test_get_balanced_xy__empty(self):
        balancer = LearnDataBalancer()
        x, y = balancer.get_balanced_xy()
        self.assertTrue(x.empty)
        self.assertTrue(y.empty)

    def test_get_balanced_xy__zero_max_len(self):
        balancer = self.balancer_with_data()
        balancer.max_len = 0
        x, y = balancer.get_balanced_xy()
        self.assertTrue(x.empty)
        self.assertTrue(y.empty)

    def test_get_balanced_xy__unbalanced_empty(self):
        # Prepare unbalanced data
        balancer = self.balancer_with_data()
        balancer.x_dict[1], balancer.y_dict[1] = pd.DataFrame(), pd.DataFrame()

        # Call
        x, y = balancer.get_balanced_xy()

        # Absense of one signal data should result empty output
        self.assertTrue(x.empty)
        self.assertTrue(y.empty)

    def test_get_balanced_xy__balanced(self):
        # Prepare unbalanced data
        balancer = self.balancer_with_data()
        # Call
        x, y = balancer.get_balanced_xy()

        # Assert max item is balanced
        self.assertEqual([[2], [3], [4], [5], [6], [7], [8], [9], [10]], x.values.tolist())
        self.assertEqual([[20], [30], [40], [50], [60], [70], [80], [90], [100]], y.values.tolist())

    def test_add__all_signals(self):
        balancer = LearnDataBalancer()
        balancer.max_len = 1
        times = [
            datetime.datetime(2023, 10, 1, 13, 5, 1),
            datetime.datetime(2023, 10, 1, 13, 5, 2),

            datetime.datetime(2023, 10, 1, 13, 5, 3),
            datetime.datetime(2023, 10, 1, 13, 5, 4),

            datetime.datetime(2023, 10, 1, 13, 5, 5),
            datetime.datetime(2023, 10, 1, 13, 5, 6),
        ]

        x = pd.DataFrame(data=["feature1", "feature2", "feature3", "feature4", "feature5", "feature6"],
                         columns=["feature"],
                         index=times)

        y = pd.DataFrame(data=[-1, -1, 0, 0, 1, 1],
                         columns=["signal"],
                         index=times)
        balancer.add(x, y)

        # Should keep only last single item per signal
        self.assertListEqual([["feature2"]], balancer.x_dict[-1].values.tolist())
        self.assertListEqual([[-1]], balancer.y_dict[-1].values.tolist())

        self.assertListEqual([["feature4"]], balancer.x_dict[0].values.tolist())
        self.assertListEqual([[0]], balancer.y_dict[0].values.tolist())

        self.assertListEqual([["feature6"]], balancer.x_dict[1].values.tolist())
        self.assertListEqual([[1]], balancer.y_dict[1].values.tolist())

    def test_add__one_signal(self):
        balancer = LearnDataBalancer()
        balancer.max_len = 1
        times = [
            datetime.datetime(2023, 10, 1, 13, 5, 1),
            datetime.datetime(2023, 10, 1, 13, 5, 2)
        ]

        x = pd.DataFrame(data=["feature1", "feature2"],
                         columns=["feature"],
                         index=times)

        y = pd.DataFrame(data=[-1, -1],
                         columns=["signal"],
                         index=times)
        balancer.add(x, y)

        # Should keep only -1 signal
        self.assertListEqual([["feature2"]], balancer.x_dict[-1].values.tolist())
        self.assertListEqual([[-1]], balancer.y_dict[-1].values.tolist())
        self.assertTrue(balancer.x_dict[0].empty)
        self.assertTrue(balancer.y_dict[0].empty)
        self.assertTrue(balancer.x_dict[1].empty)
        self.assertTrue(balancer.y_dict[1].empty)
