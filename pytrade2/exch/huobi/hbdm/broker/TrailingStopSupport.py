from logging import Logger
from multiprocessing import RLock
from typing import Optional

from sqlalchemy.orm.session import Session

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from exch.huobi.hbdm.feed.HuobiWebSocketFeedHbdm import HuobiWebSocketFeedHbdm
from model.Trade import Trade


class TrailingStopSupport:
    """ Huobi does not have trailing stop orders, so here it is. """

    def __init__(self, conf, ws_feed: HuobiWebSocketFeedHbdm, rest_client: HuobiRestClient):
        # All variables will be redefined in child classes
        self._log: Optional[Logger] = None
        self.cur_trade: Optional[Trade] = None
        self.prev_trade: Optional[Trade] = None
        self.account_manager: Optional[AccountManagerHbdm] = None
        self.db_session: Optional[Session] = None
        self.rest_client: Optional[HuobiRestClient] = None
        self.trade_lock: Optional[RLock] = None
        self.allow_trade = False
        self.price_precision = 2
        self.ticker: Optional[str] = None
        self.ws_feed = ws_feed
        self.rest_client = rest_client
        self.ws_feed.consumers.add(self)

    def on_ticker(self, ticker: dict):
        """ Look at current price and possibly move trailing stop or close the order """

        if not self.cur_trade or not self.cur_trade.trailing_delta:
            # We are out of market or no trailing delta set in the order
            return

        if self.cur_trade.direction() == 1 and ticker["ask"] > self.cur_trade.take_profit_price:
            self.cur_trade.take_profit_price = ticker["ask"]
            self.change_sl_order(1, self.cur_trade.take_profit_price - self.cur_trade.trailing_delta)
            # Move buy trailing stop
        elif self.cur_trade.direction() == -1 and ticker["bid"] < self.cur_trade.take_profit_price:
            self.cur_trade.take_profit_price = ticker["bid"]
            self.change_sl_order(-1, self.cur_trade.take_profit_price + self.cur_trade.trailing_delta)

    def change_sl_order(self, direction: int, new_sl):
        self._log.info(f"Changing sl: {self.cur_trade.stop_loss_price}->{new_sl}")

        # Cancel old stop loss order
        res = self.rest_client.post("/linear-swap-api/v1/swap_cross_trigger_cancelall", {"pair": self.ticker})
        if res["status"] != "ok":
            self._log.error(f"Error cancelling stop loss order: {res}")
            return
        # Create new stop loss order

