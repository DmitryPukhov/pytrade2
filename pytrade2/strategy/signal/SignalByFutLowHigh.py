from strategy.signal.SignalCalcBase import SignalCalcBase


class SignalByFutLowHigh(SignalCalcBase):

    def __init__(self, profit_loss_ratio: float, stop_loss_min_coeff: float, stop_loss_max_coeff: float,
                 take_profit_min_coeff: float, take_profit_max_coeff: float, comission_pct: float,
                 price_presision: float):
        super().__init__(profit_loss_ratio, stop_loss_min_coeff, stop_loss_max_coeff, take_profit_min_coeff,
                         take_profit_max_coeff, price_presision)
        self.comission_pct = comission_pct

    def calc_signal(self, close, low, high, fut_low, fut_high) -> (int, float, float):
        """ @:return Signal, stop loss, take profit """
        # BTC-USDT 40 000 * 1% = 400
        # BTC-USDT 40 000 * 0.012% = 40 * 0.012 = 4,8
        # comission = comission_pct*0.01
        comiss_abs = close * self.comission_pct * 0.01 * 2
        # Ratio to open: generate signal if profit/loss > open ratio
        # open_ratio = 1
        min_profit = close * min(self.comission_pct * 0.01 * 2, self.take_profit_min_coeff)
        max_loss = close * self.stop_loss_max_coeff

        # todo: calc signal here
        # Profit / loss > open ratio considering comission and minimal profit
        profit_sell = (low - fut_low) - comiss_abs
        loss_sell = (fut_high - low) + comiss_abs
        signal_sell = (profit_sell > 0) & ((profit_sell / loss_sell) > self.profit_loss_ratio) & (
                profit_sell > min_profit) & (loss_sell < max_loss)

        # Profit / loss > open ratio considering comission and minimal profit
        profit_buy = (fut_high - high) - comiss_abs
        loss_buy = (high - fut_low) + comiss_abs
        signal_buy = (profit_buy > 0) & ((profit_buy / loss_buy) > self.profit_loss_ratio) & (
                profit_buy > min_profit) & (loss_buy < max_loss)

        signal, sl, tp = 0, None, None
        if signal_buy and not signal_sell:
            signal, sl, tp = 1, round(fut_low, self.price_precision), round(fut_high, self.price_precision)
        elif signal_sell and not signal_buy:
            signal, sl, tp = -1, round(fut_high, self.price_precision), round(fut_low, self.price_precision)
        else:
            signal, sl, tp = 0, None, None
        return signal, sl, tp
