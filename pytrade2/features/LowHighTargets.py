import numpy as np
import pandas as pd


class LowHighTargets:
    @staticmethod
    def fut_lohi(candles, predict_window: str):
        fut_lohi = pd.DataFrame(index=candles.index)
        fut_lohi[['fut_low', 'fut_high']] = (candles[['low', 'high']][::-1]
                                             .rolling(predict_window, closed='both').agg({'low': 'min', 'high': 'max'})[
                                             ::-1])
        fut_lohi['fut_low_delta'] = fut_lohi['fut_low'] - candles['close']
        fut_lohi['fut_high_delta'] = fut_lohi['fut_high'] - candles['close']
        # Drop last data with unfinished prediction windows
        fut_lohi = fut_lohi.iloc[fut_lohi.index <= max(fut_lohi.index) - pd.Timedelta(predict_window)]
        return fut_lohi[['fut_low_delta', 'fut_high_delta']]

    @staticmethod
    def fut_lohi_signal_target(df_candles_1_min: pd.DataFrame,
                               predict_window: str,
                               loss_coeff: float,
                               profit_loss_ratio: float
                               ):
        # Calc low, high for future
        df = df_candles_1_min.copy()
        df[['fut_low', 'fut_high']] = df_candles_1_min[['low', 'high']][::-1] \
                                          .rolling(predict_window, closed='left') \
                                          .agg({'low': 'min', 'high': 'max'})[::-1]

        fut_low_delta = df["fut_low"] - df["close"]
        fut_high_delta = df["fut_high"] - df["close"]
        max_loss = df["close"] * loss_coeff
        min_profit = max_loss * profit_loss_ratio
        direction = np.where(-fut_low_delta > fut_high_delta, -1,
                             np.where(fut_low_delta < fut_high_delta, 1, 0)
                             )  # 1 - buy, -1 - sell
        loss_ok = np.where(direction > 0,
                           abs(fut_low_delta) < max_loss, # Trend up, low delta is loss
                           abs(fut_high_delta) < max_loss) # Trend down, high delta is loss
        profit_ok = np.where(direction > 0,
                             fut_high_delta >= min_profit, # Trend up, high delta is profit
                             abs(fut_low_delta) >= min_profit) # Trend down, low delta is profit
        df["signal"] = direction * (loss_ok & profit_ok).astype(int)

        return df[["signal"]]
