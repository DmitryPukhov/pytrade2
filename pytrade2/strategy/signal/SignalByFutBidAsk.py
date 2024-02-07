from strategy.signal.SignalCalcBase import SignalCalcBase


class SignalByFutBidAsk(SignalCalcBase):
    """ Use bid/ask prediction to calculate signal and order params"""
    #
    # def __init__(self, profit_loss_ratio: float, stop_loss_min_coeff: float, stop_loss_max_coeff: float,
    #              take_profit_min_coeff: float, take_profit_max_coeff: float):
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

    def get_signal_sl_tp_trdelta(self, bid: float, ask: float, bid_min_fut: float, bid_max_fut: float,
                                 ask_min_fut: float,
                                 ask_max_fut: float) -> (int, float, float, float):
        """ Calculate buy, sell or nothing signal based on predictions and profit/loss ratio
        :return (<-1 for sell, 0 for none, 1 for buy>, stop loss, take profit, trailing delta)"""

        buy_profit = bid_max_fut - ask
        buy_loss = ask - bid_min_fut
        sell_profit = bid - ask_min_fut
        sell_loss = ask_max_fut - bid

        # Buy signal
        # Not zeroes and ratio is ok and max/min are ok
        is_buy_ratio = buy_profit > 0 and (buy_loss <= 0 or buy_profit / buy_loss >= self.profit_loss_ratio)
        # is_buy_loss = abs(buy_loss) < self.stop_loss_max_coeff * ask
        is_buy_loss = self.stop_loss_min_coeff * ask <= abs(buy_loss) < self.stop_loss_max_coeff * ask
        is_buy_profit = abs(buy_profit) >= self.take_profit_min_coeff * ask
        is_buy = is_buy_ratio and is_buy_loss and is_buy_profit

        # Sell signal
        # Not zeroes and ratio is ok and max/min are ok
        is_sell_ratio = sell_profit > 0 and (sell_loss <= 0 or sell_profit / sell_loss >= self.profit_loss_ratio)
        # is_sell_loss = abs(sell_loss) < self.stop_loss_max_coeff * bid
        is_sell_loss = self.stop_loss_min_coeff * bid <= abs(sell_loss) < self.stop_loss_max_coeff * bid
        is_sell_profit = abs(sell_profit) >= self.take_profit_min_coeff * bid
        is_sell = is_sell_ratio and is_sell_loss and is_sell_profit

        # This should not happen, but let's handle it and clear the flags
        if is_buy and is_sell:
            is_buy = is_sell = False

        if is_buy:
            # Buy and possibly fix the loss
            stop_loss_adj = ask - abs(buy_loss) * 1.25
            tr_delta = abs(stop_loss_adj)
            # stop_loss_adj = min(stop_loss_adj, round(ask * (1 - self.stop_loss_min_coeff), self.price_precision))
            return 1, ask, stop_loss_adj, ask + buy_profit, tr_delta
        elif is_sell:
            # Sell and possibly fix the loss
            stop_loss_adj = bid + abs(sell_loss) * 1.25
            tr_delta = abs(stop_loss_adj)
            # stop_loss_adj = max(stop_loss_adj, round(bid * (1 + self.stop_loss_min_coeff), self.price_precision))
            return -1, bid, stop_loss_adj, bid - sell_profit, tr_delta
        else:
            # No action
            return 0, None, None, None, None
