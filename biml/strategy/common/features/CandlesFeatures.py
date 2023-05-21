import pandas as pd


class CandlesFeatures:
    @staticmethod
    def candles_features_of(candles: pd.DataFrame, interval: str, window_size: int):
        cols = ["open", "high", "low", "close", "vol"]
        features = candles.copy().reset_index(drop=False)[cols + ["close_time"]]

        # Add previous window candles to columns
        for i in range(1, window_size):
            prefix = f"{interval}_-{i}_"
            for col in cols:
                features[prefix + col] = features.shift(i)[col]
        features.set_index("close_time", inplace=True)

        # Add prefix to ohlcv columns
        for col in cols:
            features.rename(columns={col: f"{interval}_{col}"}, inplace=True)

        return features.dropna()
