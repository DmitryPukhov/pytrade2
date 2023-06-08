import logging
from typing import Optional

from huobi.client.market import MarketClient
from huobi.constant import TradeDirection
from huobi.model.market import TradeDetail
from huobi.model.market.trade_detail_event import TradeDetailEvent

from exch.huobi.HuobiTools import HuobiTools
from model.Trade import Trade
from model.TradeStatus import TradeStatus


class TrailingStopSupport:
    """ Programmatically support trailing stop because Huobi does not have oco orders???  for sl/tp"""

    def __init__(self):
        self._log = logging.getLogger(self.__class__.__name__)
        self.cur_trade: Optional[Trade] = None
        self.market_client: MarketClient = None
        self.trade_lock = None

    def sub_events(self, symbol: str):
        """ Subscribe to price changing events to track takeprofit """
        self.market_client.sub_trade_detail(symbols=symbol,
                                            callback=self.on_price_changed,
                                            error_handler=self.tss_error_callback)
        self._log.debug(f"Subscribed to price changed events for {symbol}")

    def tss_error_callback(self, ex):
        self._log.error(HuobiTools.format_exception("TrailingStopSupport market client", ex))

    def on_price_changed(self, event: TradeDetailEvent):
        """ Price changing event, check tp"""

        if not self.cur_trade or self.cur_trade.status != TradeStatus.opened:
            return

        with self.trade_lock:
            try:
                for element in event.data:
                    # Check take profit
                    e: TradeDetail = element
                    is_buy_tp = e.direction == TradeDirection.BUY and e.price >= self.cur_trade.take_profit_price
                    is_sell_tp = e.direction == TradeDirection.SELL and e.price <= self.cur_trade.take_profit_price
                    if is_buy_tp or is_sell_tp:
                        # Reread from stock exchange, maybe stop loss already triggered
                        self.update_cur_trade_status()
                        if not self.cur_trade or self.cur_trade.status != TradeStatus.opened:
                            continue

                        self._log.info(
                            f"Triggering take profit of base {e.direction} order. "
                            f"Price: {e.price}, take profit price: {self.cur_trade.take_profit_price}, "
                            f"current trade: {self.cur_trade}")
                        # Close
                        self.close_cur_trade()
                        break
            except Exception as ex:
                self._log.error(f"on_price_changed error: {ex}")

    def update_cur_trade_status(self):
        raise NotImplementedError("update_cur_trade_status not implemented")

    def close_cur_trade(self):
        raise NotImplementedError("close_cur_trade not implemented")
