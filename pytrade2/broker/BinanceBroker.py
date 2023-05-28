import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict
from binance.spot import Spot as Client
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from broker.model.Trade import Trade


class BinanceBroker:
    """
    Orders management: buy, sell etc
    """

    def __init__(self, client: Client, config: Dict[str, str]):
        self._log = logging.getLogger(self.__class__.__name__)
        self.allow_trade = config.get("pytrade2.broker.trade.allow", False)
        self.client: Client = client

        # To convert order direction -1, 1 to buy/sell strings
        self.order_side_names = {1: "BUY", -1: "SELL"}
        self.order_side_codes = dict(map(reversed, self.order_side_names.items()))

        # Database
        self.__init_db__(config)

        # Set current opened trade if present
        self.cur_trade = self.read_last_opened_trade()
        self.update_trade_status(self.cur_trade)

        # Load saved opened trade
        if self.allow_trade:
            self.cur_trade = self.read_last_opened_trade()
            if self.cur_trade:
                self._log.info(f"Loaded previously opened current trade: {self.cur_trade}")
                self.update_trade_status(self.cur_trade)

        self.price_precision = 2
        self.min_trade_interval = timedelta(seconds=10)
        self.last_trade_time = datetime.utcnow() - self.min_trade_interval
        self._log.info(f"Completed init broker. Allow trade: {self.allow_trade}")

    def __init_db__(self, config: Dict[str, str]):
        # Create database
        strategy = config["pytrade2.strategy"]
        data_dir = config["pytrade2.data.dir"] + "/" + strategy
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        db_path = f"{data_dir}/{strategy}.db"
        self._log.info(f"Init database, path: {db_path}")
        engine = create_engine(f"sqlite:///{db_path}")
        Trade.metadata.create_all(engine)
        self.db_session = sessionmaker(engine)()

    def create_cur_trade(self, symbol: str, direction: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss_price: float,
                         take_profit_price: Optional[float]) -> Optional[Trade]:
        """
        Buy or sell with take profit and stop loss
        Binance does not support that in single order, so make 2 orders: main and stoploss/takeprofit
        """

        if not self.allow_trade:
            self._log.debug("Trading in not allowed")
            return

        if (direction not in {1, -1}) or ((datetime.utcnow() - self.last_trade_time) <= self.min_trade_interval):
            # If signal is out of market or interval is not elapsed
            return

        # Prepare vars
        self.last_trade_time = datetime.utcnow()
        side = self.order_side_names[direction]
        price = round(price, self.price_precision)
        stop_loss_price = round(stop_loss_price, self.price_precision)
        take_profit_price = round(take_profit_price, self.price_precision)

        # Main order
        self._log.info(
            f"Creating order. Asset:{symbol}  side:{side}, price: {price}  quantity: {quantity},"
            f" stop loss: {stop_loss_price}, take profit: {take_profit_price}")
        self.create_main_order(symbol=symbol,
                               side=side,
                               price=price,
                               quantity=quantity)
        if not self.cur_trade:
            return None
        # Adjust sl/tp to order filled price
        stop_loss_price_adj, take_profit_price_adj = self.adjusted_sl_tp(direction=direction,
                                                                         orig_price=price,
                                                                         orig_sl_price=stop_loss_price,
                                                                         orig_tp_price=take_profit_price,
                                                                         filled_price=self.cur_trade.open_price)
        try:
            if take_profit_price:
                # Stop loss and take profit
                self.create_sl_tp_order(
                    symbol,
                    base_direction=direction,
                    quantity=quantity,
                    base_price=self.cur_trade.open_price,
                    stop_loss_price=stop_loss_price_adj,
                    take_profit_price=take_profit_price_adj)

            else:
                # Stop loss without take profit
                self.create_sl_order(
                    symbol,
                    base_direction=direction,
                    quantity=quantity,
                    base_price=self.cur_trade.open_price,
                    stop_loss_price=stop_loss_price_adj)

        except Exception as e:
            # If sl/tp order exception, close main order
            logging.error(f"Stop loss or take profit order creation error: {e} ")
            self.create_closing_order(direction, quantity, symbol)

        # Persist order to db
        self.db_session.add(self.cur_trade)
        self.db_session.commit()

        if not self.cur_trade.close_order_id:
            # If no error, order is not closed yet
            self._log.info(f"Created new trade: {self.cur_trade}")
        else:
            self.cur_trade = None
        return self.cur_trade

    def adjusted_sl_tp(self, direction, orig_price: float, orig_sl_price: float, orig_tp_price: float,
                       filled_price: float):
        """ Main order filled price can differ from original, so change original sl/tp to filled base price"""
        stop_loss_price_adj = round(float(filled_price - direction * abs(orig_price - orig_sl_price)),
                                    self.price_precision)
        take_profit_price_adj = round(float(filled_price + direction * abs(orig_tp_price - orig_price)),
                                      self.price_precision)
        return stop_loss_price_adj, take_profit_price_adj

    def create_main_order(self, symbol: str, side: str, price: float, quantity: float):
        """ Main buy or sell order. Stop loss and take profit will be set later. """

        res = self.client.new_order(
            symbol=symbol,
            side=side,
            type="LIMIT",
            price=price,
            quantity=quantity,
            timeInForce="FOK"  # Fill or kill
        )
        self._log.debug(f"Create main order raw response: {res}")

        if res["status"] == "FILLED":
            order_id = str(res["orderId"])
            filled_price = float(res["fills"][0]["price"] if res["fills"] else price)
            open_time = datetime.utcfromtimestamp(res["transactTime"] / 1000.0)

            # Set cur trade to opened order with sl/tp
            self.cur_trade = Trade(ticker=symbol,
                                   side=side,
                                   open_time=open_time,
                                   open_price=filled_price,
                                   open_order_id=order_id,
                                   quantity=quantity)
            self._log.info(f"Created main order {self.cur_trade}")
        else:
            self._log.info(f"Cannot create main {symbol} {side} order at price {price}, that's ok.")

    def create_sl_tp_order(self, symbol: str,
                           base_direction: int,
                           quantity: float,
                           base_price: float,
                           stop_loss_price: float,
                           take_profit_price: float) -> Optional[str]:
        """
         After base order filled, create sl/tp oco order
         @:return order list id for sl/tp orders
         """
        if not self.allow_trade:
            self._log.debug("Trading in not allowed")
            return None

        other_side = self.order_side_names[-base_direction]
        stop_loss_price = round(stop_loss_price, self.price_precision)
        limit_ratio = 0.01  # 1# slippage to set stop loss limit
        stop_loss_limit_price = round(stop_loss_price - base_direction * base_price * limit_ratio, self.price_precision)
        take_profit_price = round(take_profit_price, self.price_precision)
        self._log.info(
            f"Creating stop loss and take profit for base {self.order_side_names[base_direction]} order, base_price={base_price}, stop_loss_adj={stop_loss_price},"
            f" stop_loss_limit_price={stop_loss_limit_price}, take_profit_adj={take_profit_price}")
        res = self.client.new_oco_order(
            symbol=symbol,
            side=other_side,
            quantity=quantity,
            price=take_profit_price,
            stopPrice=stop_loss_price,
            stopLimitPrice=stop_loss_limit_price,
            stopLimitTimeInForce='GTC')

        self._log.debug(f"Stop loss / take profit order raw response: {res}")

        # new_oco_order returns order list id only, so get order ids for sl and tp
        oco_res = self.client.get_oco_order(orderListId=str(res["orderListId"]))
        stop_loss_order_id = ",".join([str(sl_tp_order["orderId"]) for sl_tp_order in oco_res["orders"]])

        # Update cur trade
        self.cur_trade.stop_loss_price = stop_loss_price
        self.cur_trade.take_profit_price = take_profit_price
        self.cur_trade.stop_loss_order_id = stop_loss_order_id

    def create_sl_order(self, symbol: str,
                        base_direction: int, quantity: float,
                        base_price: float,
                        stop_loss_price: float) -> Optional[str]:

        if not self.allow_trade:
            self._log.debug("Trading in not allowed")
            return None

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
        order_id = str(res["orderId"])
        self.cur_trade.stop_loss_price = stop_loss_price
        self.cur_trade.stop_loss_order_id = order_id

        self._log.debug(f"Stop loss order raw response: {res}")

    def create_closing_order(self, direction, quantity, symbol):
        logging.info(f"Closing created {self.order_side_names[direction]} order "
                     f"with id: f{self.cur_trade.open_order_id}")
        res = self.client.new_order(
            symbol=symbol,
            side=self.order_side_names[-direction],
            type="MARKET",
            quantity=quantity
        )
        # Set current trade closure fields
        if res["status"] == "FILLED":
            self.cur_trade.close_order_id = str(res["orderId"])
            self.cur_trade.close_price = float(res["fills"][0]["price"])
            self.cur_trade.close_time = datetime.utcfromtimestamp(res["transactTime"] / 1000.0)
            self._log.info(f"New trade closed due to sl/tp error: {self.cur_trade}")
        else:
            self._log.error(f"Cannot create closing order for {self.cur_trade}")

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
        if not self.allow_trade:
            self._log.debug("Trading in not allowed")
            return trade

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
        # Try to get trade for stop loss or take profit
        for sltp_order_id in trade.stop_loss_order_id.split(","):

            # Actually a single trade or empty list will be returned by my_trades
            for close_trade in self.client.my_trades(symbol=trade.ticker, orderId=sltp_order_id):
                # Update db
                trade.close_order_id = str(close_trade["orderId"])
                trade.close_price = float(close_trade["price"])
                trade.close_time = datetime.utcfromtimestamp(close_trade["time"] / 1000.0)
                self._log.info(f"Current trade found closed, probably by stop loss or take profit: {trade}")
                self.db_session.commit()
                if trade == self.cur_trade:
                    self.cur_trade = None

        return trade
