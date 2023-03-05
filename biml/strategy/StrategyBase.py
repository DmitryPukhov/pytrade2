from AppTools import AppTools
from broker.BinanceBroker import BinanceBroker


class StrategyBase:

    def __init__(self, broker: BinanceBroker, config):
        # Binance spot client
        self.order_quantity = 0.001
        self.broker = broker
        self.tickers = AppTools.read_candles_tickers(config)
        self.ticker: str = self.tickers[-1].ticker

    def process_new_data(self):
        self.broker.update_trade_if_closed_by_stop_loss(self.broker.cur_trade)
        if self.broker.cur_trade and self.broker.cur_trade.close_time:
            self.broker.cur_trade = None

        if not self.broker.cur_trade:
            self._log.debug("No current trade found. Can calculate open signal.")
            # Get open signal because current trade does not exist, process
            (close_signal, price, stop_loss) = self.open_signal(self.candles)
            # Open new trade
            if close_signal:
                self.broker.create_cur_trade(symbol=self.ticker,
                                             order_type=close_signal,
                                             quantity=self.order_quantity,
                                             price=price,
                                             stop_loss=stop_loss)
        else:
            # Get close signal for current trade
            self._log.debug(f"Current {self.broker.cur_trade.side} trade found. Can calculate close signal.")
            # If we already are in markete, get close signal
            close_signal = self.close_signal(self.candles)
            if self.broker.cur_trade.side == self.broker.order_side_names.get(-close_signal, None):
                # Signal is opposite to current trade, so close current trade
                self.broker.end_cur_trade(symbol=self.ticker)
            elif close_signal:
                # Close signal != -signal
                self._log.debug(f"Close signal: {close_signal} "
                                f"is not opposite to current trade: {self.broker.cur_trade.side}. Nothing to do.")

    def open_signal(self, candles):
        pass

    def close_signal(self, candles):
        pass