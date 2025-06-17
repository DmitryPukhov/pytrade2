import datetime
import multiprocessing
import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from strategy.common.LSTMBidAskRegressionStrategyBase import LSTMBidAskRegressionStrategyBase


class TestLSTMBidAskRegressionStrategyBase(TestCase):
    def test_reshape_func_123_2(self):
        x = np.array([[1, 1, 1], [2, 2, 2], [3, 3, 3]])
        actual = LSTMBidAskRegressionStrategyBase.reshape_x(x, window_shape=(2, 3))
        self.assertListEqual([[[1, 1, 1], [2, 2, 2]], [[2, 2, 2], [3, 3, 3]]], actual.tolist())

    def test_reshape_func_12_2(self):
        x = np.array([[1, 1, 1], [2, 2, 2]])
        actual = LSTMBidAskRegressionStrategyBase.reshape_x(x, window_shape=(2, 3))
        self.assertListEqual([[[1, 1, 1], [2, 2, 2]]], actual.tolist())

    def test_reshape_func_1_2(self):
        x = np.array([[1, 1, 1]])
        actual = LSTMBidAskRegressionStrategyBase.reshape_x(x, window_shape=(2, 3))
        self.assertListEqual([], actual.tolist())

    def test_reshape_func_2_2(self):
        x = np.array([[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]])
        actual = LSTMBidAskRegressionStrategyBase.reshape_x(x, window_shape=(2, 3))
        self.assertListEqual([[[1, 1, 1], [2, 2, 2]], [[2, 2, 2], [3, 3, 3]], [[3, 3, 3], [4, 4, 4]]], actual.tolist())

    def test_reshape_func_1_1(self):
        x = np.array([[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]])
        actual = LSTMBidAskRegressionStrategyBase.reshape_x(x, window_shape=(1, 3))
        self.assertListEqual([[[1, 1, 1]], [[2, 2, 2]], [[3, 3, 3]], [[4, 4, 4]]], actual.tolist())

    def test_reshape_func_1_2(self):
        x = np.array([[1, 1, 1]])
        actual = LSTMBidAskRegressionStrategyBase.reshape_x(x, window_shape=(10, 3))
        self.assertListEqual([], actual.tolist())
