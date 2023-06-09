import logging
import threading
from datetime import datetime
from io import StringIO
from typing import Dict, Optional

import pandas as pd
from huobi.client.account import AccountClient
from huobi.client.algo import AlgoClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
from huobi.constant import *
from huobi.model.trade import OrderUpdateEvent

from exch.BrokerBase import BrokerBase
from exch.huobi.HuobiTools import HuobiTools
from exch.huobi.broker.AccountManager import AccountManager
from exch.huobi.broker.TrailingStopSupport import TrailingStopSupport
from model.Trade import Trade
from model.TradeStatus import TradeStatus


class HuobiBroker(BrokerBase, TrailingStopSupport):
    """ Trading functions for Huobi """

    def __init__(self, config: Dict[str, str],
                 market_client: MarketClient,
                 account_client: AccountClient,
                 trade_client: TradeClient,
                 algo_client: AlgoClient,
                 ):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.trade_lock = threading.RLock()

        self.trade_client = trade_client
        self.algo_client = algo_client
        self.account_client = account_client
        self.market_client = market_client

        self.account_manager = AccountManager(account_client=account_client, config=config)

        self.account_id = int(config["pytrade2.broker.huobi.account.id"])

        self.subscribed_symbols = set()
        for ticker in config["pytrade2.tickers"].split(","):
            self.sub_events(ticker.lower())

        # Read last opened trade etc in base class
        super().__init__(config)

    def order_amount_of(self, direction: int, ticker: str, base_quantity: float) -> float:
        """
        If sell, order amount is in base currency (btc for btcusdt),
        if buy - in second currency (usdt for btcusdt)
        That's what exchange expects in create_order() call
        """
        if direction == 1:
            # Recalculate buy amount using last bid
            amount = round(self.market_client.get_market_detail_merged(ticker).bid[0] * base_quantity,
                           self.amount_precision)
            self._log.debug(
                f"Recalculated {ticker} amount, direction:{direction}, quantity:{base_quantity} -> {amount}")
        else:
            # Leave as is
            amount = base_quantity
        return amount

    def sub_events(self, symbol: str):
        if symbol not in self.subscribed_symbols:
            self.subscribed_symbols.add(symbol)
            self.trade_client.sub_order_update(symbols=symbol,
                                               callback=self.on_order_update,
                                               error_handler=self.trade_client_error_callback)
            TrailingStopSupport.sub_events(self, symbol)
            self.account_manager.sub_events()

            self._log.debug(f"Subscribed to order update events for {symbol}")

    def trade_client_error_callback(self, ex):
        self._log.error(HuobiTools.format_exception("Trade client", ex))

    def create_order(self, symbol: str, direction: int, price: float, quantity: float) -> Optional[Trade]:
        """ Make the order, return filled trade for the order"""
        with self.trade_lock:
            symbol = symbol.lower()
            self.sub_events(symbol)

            # Calculate huobi order type
            if direction == 1:
                # order_type = OrderType.BUY_MARKET
                order_type = OrderType.BUY_LIMIT_FOK
            elif direction == -1:
                # order_type = OrderType.SELL_MARKET
                order_type = OrderType.SELL_LIMIT_FOK
            else:
                order_type = 0

            amount = quantity  # Don't recalculate btc/usdt
            # amount = self.order_amount_of(direction=direction, ticker=symbol, base_quantity=quantity)

            # Make order using trade client
            # order_id is always returned, exception otherwise
            order_id = self.trade_client.create_order(
                symbol=symbol,
                account_id=self.account_id,
                order_type=order_type,
                amount=amount,
                price=price,
                source=OrderSource.API
            )
            self._log.debug(f"Created order with id:{order_id}")

            # Create trade to return
            order = self.trade_client.get_order(order_id)
            if order.state == OrderState.FILLED:
                # Set cur trade to opened order with sl/tp
                trade = Trade(ticker=symbol,
                              side=Trade.order_side_names[direction],
                              open_time=datetime.utcfromtimestamp(order.created_at / 1000.0),
                              open_price=float(order.price),
                              open_order_id=order_id,
                              quantity=float(order.amount))
            else:
                trade = None
            return trade

    def create_sl_tp_order(self, base_trade: Trade,
                           stop_loss_price: float,
                           stop_loss_limit_price: float,
                           take_profit_price: float) -> Trade:
        """ Stop loss + take profit order """
        with self.trade_lock:
            # Calculate huobi order type
            if base_trade.direction() == 1:
                sl_order_type = OrderType.SELL_STOP_LIMIT
                operator = "lte"  # operator for stop price
            elif base_trade.direction() == -1:
                sl_order_type = OrderType.BUY_STOP_LIMIT
                operator = "gte"  # operator for stop price
            else:
                sl_order_type, operator = 0, None  # should never come here

            amount = base_trade.quantity  # Don't recalculate amount
            # If buy, amount should be in the second currency in pair.
            # amount = self.order_amount_of(direction=base_trade.direction(), ticker=base_trade.ticker,
            #                               base_quantity=base_trade.quantity)

            # Trade client we stop loss???
            sl_tp_order_id = self.trade_client.create_order(
                symbol=base_trade.ticker,
                account_id=self.account_id,
                order_type=sl_order_type,
                amount=amount,
                price=stop_loss_limit_price,
                stop_price=stop_loss_price,
                source=OrderSource.API,
                operator=operator
            )

            sl_tp_order = self.trade_client.get_order(sl_tp_order_id)
            if sl_tp_order.state != OrderState.CANCELED:
                self._log.debug(f"Created sl/tp order, id: {sl_tp_order_id}")
                base_trade.stop_loss_order_id = sl_tp_order_id
                base_trade.stop_loss_price = float(stop_loss_price)
                base_trade.take_profit_price = float(take_profit_price)
            return base_trade

    def create_sl_order(self, base_trade: Trade,
                        stop_loss_price: float,
                        stop_loss_limit_price: float) -> Optional[Trade]:
        """ Stop loss order without take profit"""
        with self.trade_lock:
            # Calculate huobi order type
            if base_trade.direction() == 1:
                sl_order_type = OrderType.SELL_STOP_LIMIT
            elif base_trade.direction() == -1:
                sl_order_type = OrderType.BUY_STOP_LIMIT
            else:
                sl_order_type = 0

            # If buy, amount should be in the second currency in pair.
            # amount = self.order_amount_of(direction=base_trade.direction(), ticker=base_trade.ticker,
            #                               base_quantity=base_trade.quantity)

            amount = base_trade.quantity  # Don't recalculate quantity

            # Trade client we stop loss???
            sl_tp_order_id = self.trade_client.create_order(
                symbol=base_trade.ticker,
                account_id=self.account_id,
                order_type=sl_order_type,
                amount=amount,
                price=stop_loss_limit_price,
                stop_price=stop_loss_price,
                source=OrderSource.API)

            self._log.debug(f"Created sl/tp order, id: {sl_tp_order_id}")
            base_trade.stop_loss_order_id = sl_tp_order_id
            base_trade.stop_loss_price = stop_loss_price
            return base_trade

    def create_closing_order(self, trade: Trade):
        with self.trade_lock:
            trade.status = TradeStatus.closing
            # Closed stop loss order if not closed
            if trade.stop_loss_order_id:
                try:
                    self.trade_client.cancel_order(symbol=trade.ticker, order_id=trade.stop_loss_order_id)
                except Exception as e:
                    self._log.error(
                        f"Cannot cancel stop loss order, maybe stop loss already cancelled. Trade: {trade}, error:{e}")

            # Close main order
            # Get latest price to use in the order
            md = self.market_client.get_market_detail_merged(trade.ticker)
            lastbid, lastask = md.bid[0], md.ask[0]
            self._log.debug(f"Got last {trade.ticker} bid: {lastbid}, ask: {lastask}")

            base_direction = Trade.order_side_codes[trade.side]
            if base_direction == 1:
                # Main order was buy, closing will be sell
                close_order_type = OrderType.SELL_LIMIT
                price = round(lastbid * (1 - 0.01), self.price_precision)
            elif base_direction == -1:
                # Main order was sell, closing will be buy
                close_order_type = OrderType.BUY_LIMIT
                price = round(lastask * (1 + 0.01), self.price_precision)
            else:
                close_order_type = None

            # If buy, amount should be in the second currency in pair.
            # amount = self.order_amount_of(direction=-trade.direction(), ticker=trade.ticker,
            #                               base_quantity=trade.quantity)
            amount = trade.quantity
            self._log.debug(
                f"Creating {trade.ticker} {close_order_type} order to close main one, amount: {amount}, price: {price}")
            close_order_id = self.trade_client.create_order(
                symbol=trade.ticker,
                account_id=self.account_id,
                order_type=close_order_type,
                amount=amount,
                price=price,
                source=OrderSource.API)
            #
            trade.close_order_id = close_order_id
            self._log.debug(f"Created closure order, id: {trade.close_order_id}")

            # Get closure order details
            close_order = self.trade_client.get_order(close_order_id)
            if close_order.state == OrderState.FILLED:
                self._log.debug(f"Closure order {trade.close_order_id} at price {close_order.price} filled. "
                                f"Filled amount: {close_order.filled_amount}, cache amount: {close_order.filled_cash_amount}")
                trade.close_order_id = close_order_id
                trade.close_price = float(close_order.price)
                trade.close_time = datetime.utcfromtimestamp(close_order.finished_at / 1000.0)

                # Final closure will be not here when  on_order_update event triggered
                # trade.status = TradeStatus.closed
            return trade

    def update_cur_trade_status(self):
        """ If given trade closed by stop loss, update db and set cur trade variable to none """
        if not self.cur_trade or self.cur_trade.status == TradeStatus.closed or not self.cur_trade.stop_loss_order_id:
            return

        with self.trade_lock:
            try:
                # Try to get trade for stop loss or take profit
                for sltp_order_id in self.cur_trade.stop_loss_order_id.split(","):
                    # Actually a single trade or empty list will be returned by my_trades
                    close_order = self.trade_client.get_order(order_id=int(self.cur_trade.stop_loss_order_id))
                    if close_order.state == OrderState.FILLED and not self.cur_trade.close_order_id:
                        # Close trade in db
                        self.cur_trade.close_order_id = sltp_order_id
                        self.cur_trade.close_price = float(close_order.price)
                        self.cur_trade.close_time = datetime.utcfromtimestamp(close_order.finished_at / 1000.0)
                        self.cur_trade.status = TradeStatus.closed

                        self.db_session.commit()
                        self.cur_trade = None
            except Exception as e:
                self._log.error(f"Error updating status of the trade: {self.cur_trade}. Error: {e}")

    def on_order_update(self, event: OrderUpdateEvent):
        """ Update current trade prices from filled main or sl/tp order"""

        with self.trade_lock:
            try:
                order = event.data
                order_time = datetime.utcfromtimestamp(order.tradeTime / 1000.0)
                self._log.debug(f"{order.symbol} {order.type} update event. Order id:{order.orderId}, "
                                f"status:{order.orderStatus} price: {order.tradePrice}, time:{order_time}")
                if not self.cur_trade or order.orderStatus != OrderState.FILLED:
                    # This update is not about filling current trade
                    self._log.debug(f"This update of order with id:{order.orderId} "
                                    f"is not about final filling current trade: {self.cur_trade}")
                    return

                if self.cur_trade.id == str(order.orderId):
                    # Main order filled
                    self.cur_trade.open_price = order.tradePrice
                    self.cur_trade.open_time = order_time
                    self.cur_trade.status = TradeStatus.opened
                    self._log.info(f"Got current trade opened event: {self.cur_trade}")
                    # Save to db
                    self.db_session.commit()
                elif self.cur_trade.stop_loss_order_id == order.orderId \
                        or self.cur_trade.close_order_id == str(order.orderId):
                    # Stop loss, take profit or closure order filled
                    self.cur_trade.close_price = order.tradePrice
                    self.cur_trade.close_time = order_time
                    self.cur_trade.status = TradeStatus.closed  # Final closure is here
                    # Save to db
                    self.db_session.commit()
                    self._log.info(f"Got current trade closed event: {self.cur_trade}")
                    self.cur_trade = None
                else:
                    self._log.debug(f"This update of order id: {order.orderId} "
                                    f"is not opening or closing cur trade: {self.cur_trade}")
            except Exception as ex:
                self._log.error(f"on_order_update error:{ex}")

    def get_report(self):
        """ Short info for report """

        # Form message string
        msg = StringIO()
        msg.write(f"Allow trade: {self.allow_trade}\n")

        # Opened trade
        msg.write(f"Current trade: {self.cur_trade}\n")

        try:
            # Opened orders
            for symbol in self.subscribed_symbols:
                opened_orders = self.trade_client.get_open_orders(symbol=symbol, account_id=self.account_id)
                for order in opened_orders:
                    msg.write(f"Opened order: {symbol}, id: {order.id}, type:{order.type}, state: {order.state}, "
                              f"created:{pd.to_datetime(order.created_at, unit='ms')}, price:{order.price}")
                    msg.write("\n")
        except Exception as e:
            self._log.error(f"Error reporting opened orders: {e}")

        try:
            # Account balance
            balance = self.account_client.get_balance(self.account_id)
            actual_balance = "\n".join(
                [f"{b.currency} amount: {b.balance}, type: {b.type}" for b in balance if float(b.balance) > 0])
            msg.write(actual_balance)
        except Exception as e:
            self._log.error(f"Error reporting account info: {e}")

        return msg.getvalue()
