from AppTools import AppTools
from broker.BinanceBroker import BinanceBroker


class StrategyBase:

    def __init__(self, broker: BinanceBroker, config):
        # Binance spot client
        self.order_quantity = 0.001
        self.broker = broker
        self.tickers = AppTools.read_candles_tickers(config)
        self.ticker: str = self.tickers[-1].ticker

    def process_signal(self, signal: int, price: float, stop_loss: float):
        """ Create new trade or close current """
        if not signal:
            return
        # Update broker from exchange
        self.broker.clear_cur_trade_if_closed()

        if not self.broker.cur_trade:
            # Open new trade
            self.broker.new_trade(symbol=self.ticker, side=signal, quantity=self.order_quantity, price=price,
                                  stop_loss=stop_loss)
        elif signal == -self.broker.cur_trade:
            # Close opened trade
            self.broker.end_cur_trade(symbol=self.ticker)
