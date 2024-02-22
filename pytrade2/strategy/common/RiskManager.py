import logging
from datetime import datetime
from typing import Optional

from exch.Broker import Broker


class RiskManager:
    """ Risk management. Deny trading after bad trades """

    def __init__(self, broker, wait_after_loss):
        self._logger = logging.getLogger(self.__class__.__name__)

        self._broker: Broker = broker
        self._wait_after_loss = wait_after_loss

    def can_trade(self, cur_time: Optional[datetime] = None):
        if not self._broker.prev_trade:
            return True
        if not cur_time:
            cur_time = datetime.utcnow()
        fee = self._broker.fee
        trade = self._broker.prev_trade
        profit = float(trade.direction()) * (trade.close_price - trade.open_price)
        profit -= fee*(trade.open_price + trade.close_price)
        deltatime = cur_time - trade.close_time
        return profit >= 0 or deltatime >= self._wait_after_loss
