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
        self.is_trailing_stop = bool(conf["pytrade2.order.trailingstop"])
        if self.is_trailing_stop:
            self.ws_feed.consumers.add(self)

    def on_ticker(self, ticker: dict):
        if not self.cur_trade or not self.is_trailing_stop:
            return

        if self.cur_trade.direction() == 1 and self.cur_trade.take_profit_price >= ticker["bid"]:
            # Got buy take profit
            pass
        elif self.cur_trade.direction() == -1 and self.cur_trade.take_profit_price <= ticker["ask"]:
            # Got sell take profit
            pass

    def change_sltp_order(self, direction: int, new_sl, new_tp):
        self._log.info(f"Changing sl: {self.cur_trade.stop_loss_price}->{new_sl}")
        self.rest_client.post("/linear-swap-api/v1/swap_cross_trigger_cancelall", {"pair": self.ticker})
