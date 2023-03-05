import logging
from datetime import datetime
from typing import Optional

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
            self._log.info(f"Current last opened trade: {self.cur_trade}")
        self._log.info("Completed init broker")

    def __init_db__(self):
        # Create database
        db_path = "../data/biml.db"
        self._log.info(f"Init database, path: {db_path}")
        engine = create_engine(f"sqlite:///{db_path}")
        Trade.metadata.create_all(engine)
        self.db_session = sessionmaker(engine)()
        self.cur_trade = self.read_last_opened_trade()

    def create_cur_trade(self, symbol: str, order_type: int,
                         quantity: float,
                         price: Optional[float],
                         stop_loss: Optional[float]) -> Optional[Trade]:
        """
        Buy or sell with take profit and stop loss
        Binance does not support that in single order, so make 2 orders: main and stoploss/takeprofit
        """

        if not order_type:
            return None
        ticker_size = 2

        side = self.order_side_names[order_type]
        self._log.info(
            f"Creating order. Asset:{symbol}  side:{side}, price: {price}  quantity: {quantity}, stop loss: {stop_loss}")
        # Main buy or sell order with stop loss
        # This works
        res = self.client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=quantity
        )
        filled_price = float(res["fills"][0]["price"] if res["fills"] else price)
        order_id, stop_loss_order_id = res["orderId"], None
        if not filled_price:
            raise Exception(f"New order filled_price is empty: {res}")

        # stop loss
        if price and stop_loss:
            other_side = self.order_side_names[-order_type]
            stop_loss = round(stop_loss, ticker_size)
            stop_loss_limit_price = round(stop_loss - (price - stop_loss), ticker_size)
            self._log.info(
                f"Creating stop loss order, stop_loss={stop_loss}, stop_loss_limit_price={stop_loss_limit_price}")
            res = self.client.new_order(
                symbol=symbol,
                side=other_side,
                type="STOP_LOSS_LIMIT",
                stopPrice=stop_loss,  # When stopPrice is reached, place limit order with price.
                price=stop_loss_limit_price,  # Order executed with this price or better (ideally stopPrice)
                trailingDelta=200,  # 200 bips=2%
                timeInForce="GTC",
                quantity=quantity)
            stop_loss_order_id = res["orderId"]
            self._log.debug(f"Stop loss order response: {res}")

        self.cur_trade = Trade(ticker=symbol, side=side,
                               open_time=datetime.utcnow(), open_price=filled_price, open_order_id=order_id,
                               stop_loss_price=stop_loss, stop_loss_order_id=stop_loss_order_id,
                               quantity=quantity)
        self.db_session.add(self.cur_trade)
        self.db_session.commit()
        self._log.info(f"Created new trade: {self.cur_trade}")
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
        self.update_trade_if_closed_by_stop_loss(trade)

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
            trade.close_time = datetime.now()
            trade.close_price = filled_price

            self.db_session.commit()
        return trade

    def update_trade_if_closed_by_stop_loss(self, trade: Trade) -> Trade:
        """ If given trade closed by stop loss, update db and set cur trade variable to none """

        if not trade or trade.close_time:
            return trade
        # Get single closing trade or None
        close_trade = (self.client.my_trades(symbol=trade.ticker, orderId=trade.stop_loss_order_id)
                       or [None])[-1]
        if close_trade:
            self._log.debug(f"Current trade found closed by stop loss or take profit: {trade}")
            # Update db
            trade.close_order_id = trade.stop_loss_order_id
            trade.close_price = close_trade["price"]
            trade.close_time = datetime.utcfromtimestamp(close_trade["time"] / 1000.0)
            self.db_session.commit()
        return trade
