from datetime import datetime

from strategy.signal.SignalCalcBase import SignalCalcBase


class SignalByFutLowHigh(SignalCalcBase):

    def __init__(self, profit_loss_ratio: float, stop_loss_min_coeff: float, stop_loss_max_coeff: float,
                 take_profit_min_coeff: float, take_profit_max_coeff: float, comission_pct: float,
                 price_presision: float):
        super().__init__(profit_loss_ratio, stop_loss_min_coeff, stop_loss_max_coeff, take_profit_min_coeff,
                         take_profit_max_coeff, price_presision)
        self.comission_pct = comission_pct

    def calc_signal(self, close, fut_low, fut_high) -> (int, float, float):
        signal_ext = self.calc_signal_ext(close, fut_low, fut_high)
        return signal_ext['signal'], signal_ext['sl'], signal_ext['tp']

    def calc_signal_ext(self, close, fut_low, fut_high) -> {}:
        """ @:return Signal, stop loss, take profit """
        # BTC-USDT 40 000 * 1% = 400
        # BTC-USDT 40 000 * 0.012% = 40 * 0.012 = 4,8
        # comission = comission_pct*0.01
        comiss_abs = close * self.comission_pct * 0.01 * 2
        # Ratio to open: generate signal if profit/loss > open ratio
        min_profit = close * min(self.comission_pct * 0.01 * 2, self.take_profit_min_coeff)
        max_loss = close * self.stop_loss_max_coeff

        # Profit / loss > open ratio considering comission and minimal profit
        profit_sell = (close - fut_low) - comiss_abs
        loss_sell = max(0, fut_high - close) + comiss_abs
        sell_ratio = (profit_sell / loss_sell) if loss_sell else float('inf') if profit_sell else 0
        signal_sell = ((profit_sell > 0) & (sell_ratio >= self.profit_loss_ratio) &
                       (profit_sell > min_profit) & (loss_sell < max_loss))

        # Profit / loss > open ratio considering comission and minimal profit
        profit_buy = (fut_high - close) - comiss_abs
        loss_buy = max(0, close - fut_low) + comiss_abs
        buy_ratio = profit_buy / loss_buy if loss_buy else float('inf') if profit_buy else 0
        signal_buy = (profit_buy > 0) & (buy_ratio >= self.profit_loss_ratio) & (
                profit_buy > min_profit) & (loss_buy < max_loss)

        signal, sl, tp = 0, None, None
        if signal_buy and not signal_sell:
            # Apply minimal possible stop loss, maximum possible take profit
            sl_adj = min(fut_low, close * (1 - self.stop_loss_min_coeff))
            tp_adj = min(fut_high, close * (1 + self.take_profit_max_coeff))
            signal, sl, tp = 1, round(sl_adj, self.price_precision), round(tp_adj, self.price_precision)
        elif signal_sell and not signal_buy:
            # Apply minimal possible stop loss, maximum possible take profit
            sl_adj = max(fut_high, close * (1 + self.stop_loss_min_coeff))
            tp_adj = max(fut_low, close * (1 - self.take_profit_max_coeff))
            signal, sl, tp = -1, round(sl_adj, self.price_precision), round(tp_adj, self.price_precision)
        else:
            signal, sl, tp = 0, None, None
        return {
            'datetime': datetime.now(),
            'signal': signal,
            'sl': sl,
            'tp': tp,
            'price': round(close, self.price_precision),
            'fut_low': round(fut_low, self.price_precision),
            'fut_high': round(fut_high, self.price_precision),
            'signal_buy': signal_buy,
            'profit_buy': round(profit_buy, self.price_precision),
            'loss_buy': round(loss_buy, self.price_precision),
            'buy_ratio': round(buy_ratio, self.price_precision),
            'signal_sell': signal_sell,
            'profit_sell': round(profit_sell, self.price_precision),
            'loss_sell': round(loss_sell, self.price_precision),
            'sell_ratio': round(sell_ratio, self.price_precision)
        }
