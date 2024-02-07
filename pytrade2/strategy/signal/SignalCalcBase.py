class SignalCalcBase:
    def __init__(self, profit_loss_ratio: float, stop_loss_min_coeff: float, stop_loss_max_coeff: float,
                 take_profit_min_coeff: float, take_profit_max_coeff: float):
        self.profit_loss_ratio = profit_loss_ratio

        # stop loss should be above price * min_stop_loss_coeff
        # 0.00005 for BTCUSDT 30000 means 1,5
        self.stop_loss_min_coeff = stop_loss_min_coeff

        # 0.005 means For BTCUSDT 30 000 max stop loss would be 150
        self.stop_loss_max_coeff = stop_loss_max_coeff
        # 0.002 means For BTCUSDT 30 000 max stop loss would be 60
        self.take_profit_min_coeff = take_profit_min_coeff
        self.take_profit_max_coeff = take_profit_max_coeff