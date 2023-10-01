from datetime import timedelta, datetime
from logging import Logger
from multiprocessing import RLock
from typing import Optional

from pandas import Timedelta
from sqlalchemy.orm.session import Session

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from exch.huobi.hbdm.broker.OrderCreator import OrderCreator
from exch.huobi.hbdm.feed.HuobiWebSocketFeedHbdm import HuobiWebSocketFeedHbdm
from model.Trade import Trade


class TrailingStopSupport:
    """ Huobi does not have trailing stop orders, so here it is. """

    def __init__(self, conf, ws_feed: HuobiWebSocketFeedHbdm, rest_client: HuobiRestClient):
        # All variables will be redefined in child classes
        self.cur_trade: Optional[Trade] = None
        self.prev_trade: Optional[Trade] = None
        self.account_manager: Optional[AccountManagerHbdm] = None
        self.db_session: Optional[Session] = None
        self.rest_client: Optional[HuobiRestClient] = None
        self.trade_lock: Optional[RLock] = None
        self.allow_trade = False
        self.price_precision = 2
        self.min_trade_timedelta: timedelta = timedelta(seconds=1)
        self.last_ts_move_time = datetime.min
        self.ticker: Optional[str] = None

        # Will be initialized in child class
        self.db_session: Optional[Session] = None

        self.ws_feed = ws_feed
        self.rest_client = rest_client
        self.ws_feed.consumers.add(self)

    def on_ticker(self, ticker: dict):
        """ Look at current price and possibly move trailing stop or close the order """

        if (not self.cur_trade
                or not self.cur_trade.trailing_delta
                or (datetime.utcnow() - self.last_ts_move_time) < self.min_trade_timedelta):
            # We are out of market or no trailing delta set in the order
            return

        # Move trailing stop if needed
        if self.cur_trade.direction() == 1 and ticker["ask"] > self.cur_trade.take_profit_price:
            self.move_ts(ticker["ask"])
        elif self.cur_trade.direction() == -1 and ticker["bid"] < self.cur_trade.take_profit_price:
            self.move_ts(ticker["bid"])

    def cancel_prev_sl(self):
        """
        Cancel existing stop loss to move trailing stop
        https://www.huobi.com/en-us/opend/newApiPages/?id=8cb87edb-77b5-11ed-9966-0242ac110003
        """
        logging.info(f"Cancelling existing sl/tp orders")
        res = self.rest_client.post("/linear-swap-api/v1/swap_cross_tpsl_cancelall",
                                    {"contract_code": self.cur_trade.ticker})
        if res["status"] != "ok":
            logging.error(f"Error cancelling stop loss order: {res}")
            return

    def create_ts_order(self, new_tp: float):
        """
        Create stop loss order to move trailing stop
        https://www.huobi.com/en-us/opend/newApiPages/?id=8cb87a6f-77b5-11ed-9966-0242ac110003
        """

        # Prepare params
        t = self.cur_trade
        new_sl_trigger = new_tp - t.direction() * t.trailing_delta
        new_sl_order = OrderCreator.sl_order_price(t.direction(), new_sl_trigger)
        logging.info(f"Creating new sl order for trailing stop with new tp: {new_tp}, "
                       f"new sl trigger: {new_sl_trigger}, new sl order: {new_sl_order}")

        sl_params = OrderCreator.sl_trade_params(symbol=t.ticker,
                                                 side=Trade.order_side_names[-t.direction()],
                                                 quantity=t.quantity,
                                                 sl_trigger_price=new_sl_trigger,
                                                 sl_order_price=new_sl_order)
        # Place the order
        sl_res = self.rest_client.post("/linear-swap-api/v1/swap_cross_tpsl_order", sl_params)

        if "status" in sl_res and sl_res["status"] == "ok":
            self.cur_trade.take_profit_price = new_tp
            self.db_session.commit()
        else:
            logging.error(f"Error moving trailing stop: ${sl_res}")

    def move_ts(self, new_tp: float):
        """ Move trailing stop of current trade"""
        logging.info(f"Moving trailing stop to new take profit:{new_tp}")

        self.cancel_prev_sl()
        self.create_ts_order(new_tp)
        self.last_ts_move_time = datetime.utcnow()
