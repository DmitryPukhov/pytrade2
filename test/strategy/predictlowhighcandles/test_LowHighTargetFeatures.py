from datetime import datetime
from unittest import TestCase

import numpy as np
import pandas as pd

from strategy.predictlowhighcandles.LowHighCandlesFeatures import LowHighCandlesFeatures


class TestLowHighTargetFeatures(TestCase):
    # def test_target_features__signal(self):
    #     candles = pd.DataFrame(
    #         [
    #             # profit-trailing < loss*ratio, no signal
    #             {'close_time': datetime.fromisoformat('2021-12-08 07:00:00'), 'close': 10, 'fut_low': 8,
    #              'fut_high': 18},
    #             # buy profit-trailing = loss*ration, buy signal
    #             {'close_time': datetime.fromisoformat('2021-12-08 07:01:00'), 'close': 10, 'fut_low': 8,
    #              'fut_high': 19},
    #             # profit-trailing = loss*ration, sell singal
    #             {'close_time': datetime.fromisoformat('2021-12-08 07:01:00'), 'close': 10, 'fut_low': 1,
    #              'fut_high': 12},
    #         ]).set_index('close_time')
    #     withsignal = Features().targets_of(candles, loss=2, trailing=1, ratio=4)
    #     # self.assertEqual([0, 1, -1], withsignal['signal'].values.tolist())
    #     self.assertEqual([0, 1, 0], withsignal['signal_1'].values.tolist())
    #     self.assertEqual([0, 0, 1], withsignal['signal_-1'].values.tolist())
    #     self.assertEqual([1, 0, 0], withsignal['signal_0'].values.tolist())

    def test_target_features__window_should_include_current(self):
        # quotes columns: ['close_time', 'ticker', 'low', 'high', 'last', 'last_change']
        candles = pd.DataFrame(
            [{'close_time': datetime.fromisoformat('2021-12-08 07:00:00'), 'close': 3, 'low': 1, 'high': 10},
             {'close_time': datetime.fromisoformat('2021-12-08 07:01:01'), 'close': 3, 'low': 4, 'high': 6}
             ]).set_index('close_time')

        withminmax = LowHighCandlesFeatures().targets_of(candles, 1)
        self.assertEqual([-2, 1], withminmax['fut_delta_low'].values.tolist())
        self.assertEqual([9, 2], withminmax['fut_low_high_size'].values.tolist())

    def test_target_features__window_should_include_right_bound(self):
        # quotes columns: ['close_time', 'ticker', 'low', 'high', 'last', 'last_change']
        candles = pd.DataFrame([{'close_time': datetime.fromisoformat('2021-12-08 07:00:00'),
                                 'close': 3, 'low': 4, 'high': 6},
                                {'close_time': datetime.fromisoformat('2021-12-08 07:00:59'),
                                 'close': 4, 'low': 1, 'high': 10}
                                ]).set_index('close_time')

        y = LowHighCandlesFeatures().targets_of(candles, 1)
        self.assertEqual(y['fut_delta_low'].values.tolist(), [-2, -3])
        self.assertEqual(y['fut_low_high_size'].values.tolist(), [9, 9])

    def test_target_features__(self):
        # quotes columns: ['close_time', 'ticker', 'low', 'high', 'last', 'last_change']
        candles = pd.DataFrame([
            {'close_time': datetime.fromisoformat('2021-11-26 17:00:00'), 'close': 5, 'low': 4, 'high': 6},
            {'close_time': datetime.fromisoformat('2021-11-26 17:00:30'), 'close': 6, 'low': 1, 'high': 10},
            {'close_time': datetime.fromisoformat('2021-11-26 17:00:59'), 'close': 7, 'low': 3, 'high': 7},
            {'close_time': datetime.fromisoformat('2021-11-26 17:01:58'), 'close': 8, 'low': 2, 'high': 8},

            {'close_time': datetime.fromisoformat('2021-11-26 17:03:00'), 'close': 5, 'low': 4, 'high': 6},
            {'close_time': datetime.fromisoformat('2021-11-26 17:04:00'), 'close': 5, 'low': 3, 'high': 7},
            {'close_time': datetime.fromisoformat('2021-11-26 17:05:00'), 'close': 5, 'low': 4, 'high': 6},
            {'close_time': datetime.fromisoformat('2021-11-26 17:06:00'), 'close': 5, 'low': 3, 'high': 7}
        ]).set_index('close_time')

        y = LowHighCandlesFeatures().targets_of(candles, 1)

        self.assertEqual([-4, -5, -5, -6, -1, -2, -1, -2], y['fut_delta_low'].values.tolist())

        #        self.assertEqual([1, 1, 2, 2, 4, 3, 4, 3], withminmax['fut_delta_low'].values.tolist())
        self.assertEqual([9, 9, 6, 6, 2, 4, 2, 4], y['fut_low_high_size'].values.tolist())

    def test_with_predicted(self):
        """
        Update last candle with predicted values
        :param candles: candles dataframe
        :param y_pred: ndarray (1,2) with 1 preducted row and 2 columns: future low - cur close and future candle size
        :return:
        """
        candles = pd.DataFrame([
            {'close': 1},
            {'close': 2}
        ])
        y_pred = np.array([[-1,2]])
        LowHighCandlesFeatures.set_predicted_fields(candles, y_pred)

        self.assertEqual(candles.loc[candles.index[-1], "fut_low"], 1)
        self.assertEqual(candles.loc[candles.index[-1], "fut_high"], 3)
