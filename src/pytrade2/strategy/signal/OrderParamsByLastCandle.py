import pandas as pd

from pytrade2.strategy.signal.SignalCalcBase import SignalCalcBase


class OrderParamsByLastCandle(SignalCalcBase):
    # """ Fixed ratio sl/tp params, calculated from last candle"""
    # def __init__(self, profit_loss_ratio: float, stop_loss_min_coeff: float, stop_loss_max_coeff: float,
    #              take_profit_min_coeff: float, take_profit_max_coeff: float):
    #     # Expected profit/loss >= ratio means signal to trade
    #     self.profit_loss_ratio = profit_loss_ratio
    #
    #     # stop loss should be above price * min_stop_loss_coeff
    #     # 0.00005 for BTCUSDT 30000 means 1,5
    #     self.stop_loss_min_coeff = stop_loss_min_coeff
    #
    #     # 0.005 means For BTCUSDT 30 000 max stop loss would be 150
    #     self.stop_loss_max_coeff = stop_loss_max_coeff
    #     # 0.002 means For BTCUSDT 30 000 max stop loss would be 60
    #     self.take_profit_min_coeff = take_profit_min_coeff
    #     self.take_profit_max_coeff = take_profit_max_coeff

    def get_sl_tp_trdelta(self, signal: int, last_candle: pd.DataFrame) -> (float, float, float):
        sl_delta_min = self.stop_loss_min_coeff * last_candle["close"]
        sl_delta_max = self.stop_loss_max_coeff * last_candle["close"]
        tp_delta_min = self.take_profit_min_coeff * last_candle["close"]
        tp_delta_max = self.take_profit_max_coeff * last_candle["close"]
        tr_delta_min, tr_delta_max = sl_delta_min, sl_delta_max
        open_price = last_candle["close"]  # Order open price

        tr_delta = max(abs(last_candle["high"] - last_candle["low"]), tr_delta_min)  # trailing delta
        tr_delta = min(tr_delta, tr_delta_max)
        if signal == 1:
            sl = min(last_candle["low"], open_price - sl_delta_min)
            sl = max(sl, open_price - sl_delta_max)
            tp = max(last_candle["high"], open_price + tp_delta_min)
            tp = min(tp, open_price + tp_delta_max)
        elif signal == -1:
            sl = max(last_candle["high"], open_price + sl_delta_min)
            sl = min(sl, open_price + sl_delta_max)
            tp = min(last_candle["low"], open_price - tp_delta_min)
            tp = max(tp, open_price - tp_delta_max)
        else:
            # Should never come here
            return None

        return sl, tp, tr_delta
