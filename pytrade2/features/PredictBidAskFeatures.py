from typing import Dict

import numpy as np
import pandas as pd

from features.BidAskFeatures import BidAskFeatures
from features.CandlesFeatures import CandlesFeatures
from features.Level2Features import Level2Features


class PredictBidAskFeatures:
    """
    Feature engineering for bid/ask prediction strategies
    """

    # default_predict_window = "10s"
    # default_past_window = "10s"

    @staticmethod
    def last_features_of(bid_ask: pd.DataFrame, n: int,
                         level2: pd.DataFrame,
                         candles_by_interval: Dict[str, pd.DataFrame],
                         candles_cnt_by_interval: Dict[str, int],
                         past_window: str) -> pd.DataFrame:
        # Need 2 last records because features contain diff.
        last_bid_ask = bid_ask.tail(n + 1)
        last_level2 = level2[level2.index <= last_bid_ask.index.max()]
        return PredictBidAskFeatures.features_of(last_bid_ask,
                                                 last_level2,
                                                 candles_by_interval,
                                                 candles_cnt_by_interval,
                                                 past_window=past_window)

    @staticmethod
    def features_targets_of(bid_ask: pd.DataFrame,
                            level2: pd.DataFrame,
                            candles_by_interval: Dict[str, pd.DataFrame],
                            candles_cnt_by_interval: Dict[str, int],
                            predict_window: str, past_window: str) \
            -> (pd.DataFrame, pd.DataFrame):
        features = PredictBidAskFeatures.features_of(bid_ask,
                                                     level2,
                                                     candles_by_interval,
                                                     candles_cnt_by_interval,
                                                     past_window)
        targets = PredictBidAskFeatures.targets_of(bid_ask, predict_window)
        merged = pd.merge_asof(features, targets, left_index=True, right_index=True) \
            .dropna()
        features = merged[features.columns]
        targets = merged[targets.columns]
        return features, targets

    @staticmethod
    def features_of(bid_ask: pd.DataFrame,
                    level2: pd.DataFrame,
                    candles_by_interval: Dict[str, pd.DataFrame],
                    candles_cnt_by_interval: Dict[str, int],
                    past_window: str):
        if bid_ask.empty or level2.empty or not candles_by_interval:
            return pd.DataFrame()
        candles_features = CandlesFeatures.candles_combined_features_of(candles_by_interval, candles_cnt_by_interval)
        l2_features = Level2Features().level2_buckets(level2, past_window=past_window)
        bid_ask_features = pd.merge(BidAskFeatures.time_features_of(bid_ask),
                                    BidAskFeatures.bid_ask_features_of(bid_ask, past_window),
                                    left_index=True, right_index=True, sort=True)
        features = pd.merge_asof(bid_ask_features, l2_features, left_index=True, right_index=True)
        # features.drop(["symbol", "datetime"], axis=1, inplace=True)
        features = pd.merge_asof(features, candles_features, left_index=True, right_index=True)
        return features.dropna()

    @staticmethod
    def targets_of(bid_ask: pd.DataFrame, predict_window: str) -> pd.DataFrame:
        # Calculate <bid or ask>_<min or max>_fut
        rolling_max = bid_ask[["bid", "ask"]][::-1].rolling(predict_window, closed='right').max()[::-1] \
            .add_suffix("_max_fut")
        rolling_min = bid_ask[["bid", "ask"]][::-1].rolling(predict_window, closed='right').min()[::-1] \
            .add_suffix("_min_fut")
        merged = pd.concat([rolling_min, rolling_max], axis=1)
        prediction_bound = bid_ask.index.max() - pd.to_timedelta(predict_window)
        merged.loc[merged.index > prediction_bound] = [np.nan] * len(merged.columns)

        # To avoid predictions when min_fut > max_fut, targets are: bid_max_fut, bid_spread_fut, ask_min_fut, ask_spread_fut
        return pd.DataFrame({
            # Max future bid, min future ask
            "bid_max_fut_diff": merged["bid_max_fut"] - bid_ask["bid"],
            "bid_spread_fut": merged["bid_max_fut"] - merged["bid_min_fut"],

            "ask_min_fut_diff": merged["ask_min_fut"] - bid_ask["ask"],
            "ask_spread_fut": merged["ask_max_fut"] - merged["ask_min_fut"]}) \
            .dropna()
