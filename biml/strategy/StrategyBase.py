import logging
from typing import Dict, List

from broker.BinanceBroker import BinanceBroker


class StrategyBase:

    def __init__(self, broker:BinanceBroker):
        # Binance spot client
        self.order_quantity = 0.001
        self.broker = broker

    def assert_out_of_market(self, ticker: str):
        """
        Raise exception if we have opened positions for the symbol
        """
        if self.broker:
            opened_quantity, opened_orders = self.broker.opened_positions(ticker)
            if opened_quantity or opened_orders:
                raise AssertionError(
                    f"Fatal: cannot trade. We have opened positions: {opened_quantity} {ticker} and {len(opened_orders)} opened {ticker} orders.")


