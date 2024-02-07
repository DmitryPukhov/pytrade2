import pandas as pd


class LowHighTargets:
    @staticmethod
    def fut_lohi(candles, predict_window: str):
        fut_lohi = pd.DataFrame(index=candles.index)
        fut_lohi[['fut_low', 'fut_high']] = (candles[['low', 'high']][::-1]
                                             .rolling(predict_window, closed='both').agg({'low': 'min', 'high': 'max'})[::-1])
        # Drop last data with unfinished prediction windows
        fut_lohi = fut_lohi.iloc[fut_lohi.index <= max(fut_lohi.index) - pd.Timedelta(predict_window)]
        return fut_lohi
