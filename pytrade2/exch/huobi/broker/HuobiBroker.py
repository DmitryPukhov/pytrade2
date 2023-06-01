import logging
import sys
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from huobi.client.account import AccountClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
from huobi.constant import *
from huobi.model.trade import OrderUpdateEvent

from exch.BrokerBase import BrokerBase
from exch.huobi.broker.TrailingStopSupport import TrailingStopSupport
from model.Trade import Trade
from pytrade2.App import App
from pytrade2.exch.Exchange import Exchange


class HuobiBroker(BrokerBase, TrailingStopSupport):
    """ Trading functions for Huobi """

    def __init__(self, config: Dict[str, str]):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.trade_client = self._create_trade_client()
        self.account_client = self._create_account_client()
        self.market_client = self._create_market_client()

        self.account_id = int(config["pytrade2.broker.huobi.account.id"])

        self.subscribed_symbols = set()
        for ticker in config["pytrade2.tickers"].split(","):
            self.sub_events(ticker.lower())

        # Read last opened trade etc in base class
        super().__init__(config)

    def _key_secret(self):
        key = self.config["pytrade2.exchange.huobi.connector.key"]
        secret = self.config["pytrade2.exchange.huobi.connector.secret"]
        return key, secret

    def _create_market_client(self):
        key, secret = self._key_secret()
        url = self.config["pytrade2.exchange.huobi.connector.url"]
        self._log.info(f"Creating huobi market client, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
        return MarketClient(api_key=key, secret_key=secret, init_log=True)

    def _create_trade_client(self):
        """ Huobi trade client creation."""
        key, secret = self._key_secret()
        url = self.config["pytrade2.exchange.huobi.connector.url"]
        self._log.info(f"Creating huobi market client, url:{url} key: ***{key[-3:]}, secret: ***{secret[-3:]}")
        return TradeClient(url=url, api_key=key, secret_key=secret, init_log=True)

    def _create_account_client(self):
        key, secret = self._key_secret()
        # url = self.config["pytrade2.exchange.huobi.connector.url"]
        self._log.info(f"Creating huobi account client, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
        return AccountClient(api_key=key, secret_key=secret, init_log=True)

    def sub_events(self, symbol: str):
        if symbol not in self.subscribed_symbols:
            self.subscribed_symbols.add(symbol)
            self.trade_client.sub_order_update(symbols=symbol, callback=self.on_order_update)
            TrailingStopSupport.sub_events(self, symbol)
            self.market_client.sub_trade_detail(symbols=symbol, callback=self.on_price_changed)
            self._log.debug(f"Subscribed to order update events for {symbol}")

    def create_order(self, symbol: str, direction: int, price: float, quantity: float) -> Optional[Trade]:
        """ Make the order, return filled trade for the order"""
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

        # Make order using trade client
        # order_id is always returned, exception otherwise
        order_id = self.trade_client.create_order(
            symbol=symbol,
            account_id=self.account_id,
            order_type=order_type,
            amount=quantity,
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
                          open_time=pd.to_datetime(order.created_at, unit="ms"),
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
        ## todo: take profit!!!
        """ Stop loss + take profit order """
        # Calculate huobi order type
        if base_trade.direction() == 1:
            sl_order_type = OrderType.SELL_STOP_LIMIT
            operator = "lte"  # operator for stop price
        elif base_trade.direction() == -1:
            sl_order_type = OrderType.BUY_STOP_LIMIT
            operator = "gte"  # operator for stop price
        else:
            sl_order_type, operator = 0, None  # should never come here

        # Trade client we stop loss???
        sl_tp_order_id = self.trade_client.create_order(
            symbol=base_trade.ticker,
            account_id=self.account_id,
            order_type=sl_order_type,
            amount=base_trade.quantity,
            price=stop_loss_limit_price,
            stop_price=stop_loss_price,
            source=OrderSource.API,
            operator=operator
        )

        # Algo wo stop loss ??
        # sl_tp_order_id = self.client.create_order(
        #     symbol=base_trade.ticker,
        #     order_side=Trade.order_side_names[-base_trade.direction()],
        #     order_value=base_trade.quantity,
        #     stop_price=stop_loss_price,
        #     order_price=stop_loss_limit_price,
        #     take_profit_price=take_profit_price,
        #     order_type=AlgoOrderType.LIMIT
        # )
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
        """ Stop loss + take profit order """
        # Calculate huobi order type
        if base_trade.direction() == 1:
            sl_order_type = OrderType.SELL_STOP_LIMIT
        elif base_trade.direction() == -1:
            sl_order_type = OrderType.BUY_STOP_LIMIT
        else:
            sl_order_type = 0
        # Trade client we stop loss???
        sl_tp_order_id = self.trade_client.create_order(
            symbol=base_trade.ticker,
            account_id=self.account_id,
            order_type=sl_order_type,
            amount=base_trade.quantity,
            price=stop_loss_limit_price,
            stop_price=stop_loss_price,
            source=OrderSource.API)

        self._log.debug(f"Created sl/tp order, id: {sl_tp_order_id}")
        base_trade.stop_loss_order_id = sl_tp_order_id
        base_trade.stop_loss_price = stop_loss_price
        return base_trade

    def close_order(self, trade: Trade):
        base_direction = Trade.order_side_codes[trade.side]
        if base_direction == 1:
            close_order_type = OrderType.SELL_MARKET
        elif base_direction == -1:
            close_order_type = OrderType.BUY_MARKET
        else:
            close_order_type = None

        close_order_id = self.trade_client.create_order(
            symbol=trade.ticker,
            account_id=self.account_id,
            order_type=close_order_type,
            amount=trade.quantity,
            price=trade.open_price,
            source=OrderSource.API)

        if close_order_id:

            close_order = self.trade_client.get_order(close_order_id)
            if close_order.state == OrderState.FILLED:
                trade.close_order_id = close_order_id
                trade.close_price = float(close_order.price)
                trade.close_time = datetime.utcfromtimestamp(close_order.finished_at / 1000.0)
        if not trade.close_time:
            # Full stop, hanging order, requires urgent investigation
            raise f"Cannot create closing order for {trade}"
        return trade

    def update_trade_status(self, trade: Trade) -> Trade:
        """ If given trade closed by stop loss, update db and set cur trade variable to none """

        if not trade or trade.close_time or not trade.stop_loss_order_id:
            return trade

        # Try to get trade for stop loss or take profit
        for sltp_order_id in trade.stop_loss_order_id.split(","):
            # Actually a single trade or empty list will be returned by my_trades
            close_order = self.trade_client.get_order(order_id=int(trade.stop_loss_order_id))
            if close_order.state == OrderState.FILLED:
                # Update db
                trade.close_order_id = sltp_order_id
                trade.close_price = float(close_order.price)
                trade.close_time = datetime.utcfromtimestamp(close_order.finished_at / 1000.0)
        return trade

    def on_order_update(self, event: OrderUpdateEvent):
        """ Update current trade prices from filled main or sl/tp order"""

        order_time = datetime.utcfromtimestamp(event.data.tradeTime / 1000.0)
        self._log.info(f"{event.data.symbol} {event.data.type} update event. Order id:{event.data.orderId}, "
                       f"status:{event.data.orderStatus} price: {event.data.tradePrice}, time:{order_time}")
        order = event.data
        if not self.cur_trade or order.orderStatus != OrderState.FILLED:
            # This update is not about filling current trade
            return

        if self.cur_trade.id == order.orderId:
            # Main order filled
            self.cur_trade.open_price = order.tradePrice
            self.cur_trade.open_time = order_time
            self._log.info(f"Got current trade opened event: {self.cur_trade}")
            # Save to db
            self.db_session.commit()
        elif self.cur_trade.stop_loss_order_id == order.orderId:
            self.cur_trade.close_price = order.tradePrice
            self.cur_trade.close_order_id = order.orderId
            self.cur_trade.close_time = order_time
            self.db_session.commit()
            self._log.info(f"Got current trade closed event: {self.cur_trade}")
            self.cur_trade = None


class DevFunc:

    def __init__(self):

        sys.argv.append("--pytrade2.strategy")
        sys.argv.append("SimpleKerasStrategy")

        # Create broker and devfunc
        cfg = App().config
        cfg["pytrade2.broker.trade.allow"] = True
        self.broker: HuobiBroker = Exchange(cfg).broker("huobi.HuobiExchange")

    def dev_get_order_hist(self):
        for order in self.broker.trade_client.get_history_orders("btcusdt"):
            print(
                f"Order id: {order.id}, type:{order.type}, state: {order.state}, time:{pd.to_datetime(order.created_at, unit='ms')}, price:{order.price}")

    def dev_create_order_with_sl_tp(self, direction: int):
        last_price = self.dev_get_last_price()
        # price = last_price* (1- float(direction) *0.001)
        price = last_price
        sl = price * (1.0 - float(direction) * 0.003)
        tp = price * (1.0 + float(direction) * 0.003)
        self.broker.create_cur_trade(
            symbol="btcusdt",
            direction=direction,
            quantity=0.0005,  # 0.0005
            price=price,
            stop_loss_price=sl,
            take_profit_price=tp
        )

    def dev_create_huobi_order(self, direction: int):
        price = round(self.dev_get_last_price() - direction * 10 / 30000, 2)
        price = 26000
        trade = self.broker.create_order(symbol="btcusdt", direction=direction, price=price, quantity=0.0005)
        if trade:
            order = self.broker.trade_client.get_order(trade.open_order_id)
            print(f"Created {order.type} order.  {order.state}, id: {order.id}, {order.price}")
        else:
            print("Order is not filled")

    def dev_print_actual_balance(self):
        """ Get actual balance on account"""

        balance = self.broker.account_client.get_balance(self.broker.account_id)
        actual_balance = [b for b in balance if float(b.balance) > 0]
        print("Balance:")
        for b in actual_balance:
            print(f"{b.currency}: {b.balance}, type: {b.type}")
        return actual_balance

    def dev_print_orders(self):
        # # Print all orders

        opened = self.broker.trade_client.get_open_orders(symbol="btcusdt",
                                                          account_id=self.broker.account_id)
        for order in opened:
            print(f"Opened order id: {order.id}, type:{order.type}, state: {order.state}, "
                  f"time:{pd.to_datetime(order.created_at, unit='ms')}, price:{order.price}")
        state = "filled,canceled,created"
        orders = self.broker.trade_client.get_orders(symbol="btcusdt", order_state=state)

        for order in orders:
            print(
                f"Order id: {order.id}, type:{order.type}, state: {order.state}, time:{pd.to_datetime(order.created_at, unit='ms')}, price:{order.price}")

    def dev_get_last_price(self):
        # client: MarketClient = self.broker.market_client
        # detail=client.get_market_detail("btcusdt")
        lastprice = self.broker.market_client.get_market_trade("btcusdt")[-1].price
        # lastprice = self.broker.market_client.get_candlestick('btcusdt', '1min', 1)[-1].close
        print(f"Last price:{lastprice}")
        return lastprice

    def dev_get_order(self):
        order_id = 808073037255279
        o = self.broker.trade_client.get_order(order_id=order_id)
        # orders=self.broker.trade_client.get_order(order_id=order_id)
        "Closed order:"
        print(o)


# todo: remove
if __name__ == "__main__":
    devfunc = DevFunc()
    devfunc.broker.market_client.sub_candlestick()

    # devfunc.dev_create_order_with_sl_tp(1)

    # Clear current buy order
    # broker.trade_client.cancel_open_orders(account_id=broker.account_id)
    # devfunc.dev_create_huobi_order(-1)

    devfunc.dev_print_orders()
    devfunc.dev_print_actual_balance()
