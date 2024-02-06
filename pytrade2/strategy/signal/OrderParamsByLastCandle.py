import pandas as pd


class OrderParamsByLastCandle:
    """ Fixed ratio sl/tp params, calculated from last candle"""
    def __init__(self, config):
        # Expected profit/loss >= ratio means signal to trade
        self.profit_loss_ratio = config.get("pytrade2.strategy.profitloss.ratio", 1)

        # stop loss should be above price * min_stop_loss_coeff
        # 0.00005 for BTCUSDT 30000 means 1,5
        self.stop_loss_min_coeff = config.get("pytrade2.strategy.stoploss.min.coeff", 0)

        # 0.005 means For BTCUSDT 30 000 max stop loss would be 150
        self.stop_loss_max_coeff = config.get("pytrade2.strategy.stoploss.max.coeff", float('inf'))
        # 0.002 means For BTCUSDT 30 000 max stop loss would be 60
        self.profit_min_coeff = config.get("pytrade2.strategy.profit.min.coeff", 0)
        self.profit_max_coeff = config.get("pytrade2.strategy.profit.max.coeff", float('inf'))

    def get_sl_tp_trdelta(self, signal: int, last_candle: pd.DataFrame) -> (float, float, float):
        sl_delta_min = self.stop_loss_min_coeff * last_candle["close"]
        sl_delta_max = self.stop_loss_max_coeff * last_candle["close"]
        tp_delta_min = self.profit_min_coeff * last_candle["close"]
        tp_delta_max = self.profit_max_coeff * last_candle["close"]
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