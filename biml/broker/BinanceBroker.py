import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from binance.spot import Spot as Client
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from broker.model.Trade import Trade


class BinanceBroker:
    """
    Orders management: buy, sell etc
    """

    def __init__(self, client: Client):
        self._log = logging.getLogger(self.__class__.__name__)
        self.client: Client = client
        self.order_side_names = {1: "BUY", -1: "SELL"}
        self.order_side_codes = dict(map(reversed, self.order_side_names.items()))

        # Database
        self.__init_db__()
        self.cur_trade = self.read_last_opened_trade()

        if self.cur_trade:
            self._log.info(f"Loaded previously opened current trade: {self.cur_trade}")
        self._log.info("Completed init broker")
        self.price_precision = 2
        self.min_trade_interval = timedelta(seconds=10)
        self.last_trade_time = datetime.utcnow() - self.min_trade_interval

    def __init_db__(self):
        # Create database
        db_path = "../data/biml.db"
        self._log.info(f"Init database, path: {db_path}")
        engine = create_engine(f"sqlite:///{db_path}")
        Trade.metadata.create_all(engine)
        self.db_session = sessionmaker(engine)()
        self.cur_trade = self.read_last_opened_trade()

    def create_sl_tp_order(self, symbol: str,
                           base_direction: int,
                           quantity: float,
                           base_price: float,
                           stop_loss_price: float,
                           take_profit_price: float) -> str:
        """
         After base order filled, create sl/tp oco order
         @:return order list id for sl/tp orders
         """
        other_side = self.order_side_names[-base_direction]
        stop_loss_price = round(stop_loss_price, self.price_precision)
        stop_loss_limit_price = round(stop_loss_price - (base_price - stop_loss_price), self.price_precision)
        take_profit_price = round(take_profit_price, self.price_precision)
        self._log.info(
            f"Creating stop loss and take profit for base {self.order_side_names[base_direction]} order, base_price={base_price}, stop_loss={stop_loss_price},"
            f" stop_loss_limit_price={stop_loss_limit_price}, take_profit={take_profit_price}")
        res = self.client.new_oco_order(
            symbol=symbol,
            side=other_side,
            quantity=quantity,
            price=take_profit_price,
            stopPrice=stop_loss_price,
            stopLimitPrice=stop_loss_limit_price,
            stopLimitTimeInForce='GTC')
        self._log.debug(f"Stop loss / take profit order raw response: {res}")
        return str(res["orderListId"])

    def create_sl_order(self, symbol: str,
                        base_direction: int, quantity: float,
                        base_price: float,
                        stop_loss_price: float) -> str:
        sl_side = self.order_side_names[-base_direction]
        stop_loss_price = round(stop_loss_price, self.price_precision)
        stop_loss_limit_price = round(stop_loss_price - (base_price - stop_loss_price), self.price_precision)

        self._log.info(
            f"Creating stop loss for base {self.order_side_names[base_direction]} order, "
            f"stop_loss={stop_loss_price}, stop_loss_limit_price={stop_loss_limit_price}")

        res = self.client.new_order(
            symbol=symbol,
            side=sl_side,
            type="STOP_LOSS_LIMIT",
            stopPrice=stop_loss_price,  # When stopPrice is reached, place limit order with price.
            price=stop_loss_limit_price,  # Order executed with this price or better (ideally stopPrice)
            trailingDelta=200,  # 200 bips=2%
            timeInForce="GTC",
            quantity=quantity)
        self._log.debug(f"Stop loss order raw response: {res}")
        return str(res["orderId"])

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: float,
                         take_profit_price: Optional[float]) -> Optional[Trade]:
        """
        Buy or sell with take profit and stop loss
        Binance does not support that in single order, so make 2 orders: main and stoploss/takeprofit
        """

        if direction not in {1, -1}:
            return None

        if (datetime.utcnow() - self.last_trade_time) <= self.min_trade_interval:
            return

        self.last_trade_time = datetime.utcnow()
        side = self.order_side_names[direction]

        self._log.info(
            f"Creating order. Asset:{symbol}  side:{side}, price: {price}  quantity: {quantity},"
            f" stop loss: {stop_loss_price}, take profit: {take_profit_price}")
        # Main buy or sell order
        res = self.client.new_order(
            symbol=symbol,
            side=side,
            type="LIMIT",
            price=price,
            quantity=quantity,
            timeInForce="FOK"  # Fill or kill
        )
        self._log.debug(f"Create order raw response: {res}")
        if res["status"] != "FILLED":
            self._log.info(f"Cannot create {symbol} {side} order at price {price}, that's ok.")
            return

        filled_price = float(res["fills"][0]["price"] if res["fills"] else price)
        stop_loss_price_adj = filled_price - direction * abs(price - stop_loss_price)
        take_profit_price_adj = filled_price + direction * abs(take_profit_price - price)
        self._log.info(f"{side} order filled_price={filled_price}, "
                       f"stop_loss_adj={stop_loss_price_adj}, take_profit_adj={take_profit_price_adj}")
        order_id = res["orderId"]
        try:
            # stop_loss_order_id = None
            if take_profit_price:
                # Stop loss and take profit
                stop_loss_order_id = self.create_sl_tp_order(
                    symbol,
                    base_direction=direction,
                    quantity=quantity,
                    base_price=filled_price,
                    stop_loss_price=stop_loss_price_adj,
                    take_profit_price=take_profit_price_adj)
            else:
                # Stop loss without take profit
                stop_loss_order_id = self.create_sl_order(
                    symbol,
                    base_direction=direction,
                    quantity=quantity,
                    base_price=filled_price,
                    stop_loss_price=stop_loss_price_adj)

            # Set cur trade to opened order with sl/tp
            self.cur_trade = Trade(ticker=symbol,
                                   side=side,
                                   open_time=datetime.utcnow(),
                                   open_price=filled_price,
                                   open_order_id=order_id,
                                   stop_loss_price=stop_loss_price_adj,
                                   take_profit_price=take_profit_price_adj,
                                   stop_loss_order_id=stop_loss_order_id,
                                   quantity=quantity)
            self.db_session.add(self.cur_trade)
            self.db_session.commit()
            self._log.info(f"Created new trade: {self.cur_trade}")

        except Exception as e:
            # If sl/tp order exception, close main order
            logging.error(f"Stop loss or take profit order creation error: {e} ")
            logging.info(f"Closing created {self.order_side_names[direction]} order with id: f{order_id}")
            self.client.new_order(
                symbol=symbol,
                side=self.order_side_names[-direction],
                type="MARKET",
                quantity=quantity
            )

        return self.cur_trade

    def close_opened_trades(self):
        self._log.info("Closing all opened trades if exist")
        # Query opened trades
        trades = self.db_session \
            .query(Trade) \
            .where(Trade.close_time.is_(None)) \
            .order_by(Trade.open_time.desc())
        # Close opened trades
        for trade in trades:
            self.end_trade(trade)

        # If current trade was already closed,
        if self.cur_trade and self.cur_trade.close_time:
            self._log.info(f"Current trade was closed: {self.cur_trade}")
            self.cur_trade = None

    def read_last_opened_trade(self) -> Trade:
        """ Returns current opened trade, stored in db or none """
        return self.db_session \
            .query(Trade) \
            .where(Trade.close_time.is_(None)) \
            .order_by(Trade.open_time.desc()).first()

    def end_cur_trade(self):
        closed_cur_trade, self.cur_trade = self.end_trade(self.cur_trade), None
        return closed_cur_trade

    def end_trade(self, trade: Trade) -> Trade:
        """
        If current opened trade exists,  close it, error otherwise
        """
        assert trade
        self._log.info(f"Closing trade: {trade}")

        # Check if it's already closed by stop loss
        self.update_trade_status(trade)

        if trade.close_time:
            # If already closed, do nothing
            self._log.info(f"Trade {trade} is already closed")
        else:
            # Close stop loss opened orders
            if self.client.get_open_orders(trade.ticker):
                self.client.cancel_open_orders(trade.ticker)

            # Create closing order
            close_side = self.order_side_names[-self.order_side_codes[trade.side]]
            # Place closing order for current trade
            res = self.client.new_order(
                symbol=trade.ticker,
                side=close_side,
                type="MARKET",
                quantity=trade.quantity
            )

            # Update the trade with closure info
            filled_price = float(res["fills"][0]["price"] if res["fills"] else None)
            order_id = res["orderId"]

            trade.close_order_id = order_id
            trade.close_time = datetime.utcnow()
            trade.close_price = filled_price

            self.db_session.commit()
        return trade

    def update_trade_status(self, trade: Trade) -> Trade:
        """ If given trade closed by stop loss, update db and set cur trade variable to none """

        if not trade or trade.close_time:
            return trade
        last_trades = self.client.my_trades(symbol=trade.ticker, limit=3)
        last_trade = ([t for t in last_trades if str(t["orderListId"]) == trade.stop_loss_order_id] or [None])[-1]

        if last_trade:
            # Update db
            trade.close_order_id = str(last_trade["orderId"])
            trade.close_price = last_trade["price"]
            trade.close_time = datetime.utcfromtimestamp(last_trade["time"] / 1000.0)
            self._log.debug(f"Current trade found closed by stop loss or take profit: {trade}")
            self.db_session.commit()
            if trade == self.cur_trade:
                self.cur_trade = None
        return trade
