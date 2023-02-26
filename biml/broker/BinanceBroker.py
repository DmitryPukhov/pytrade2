import logging
from datetime import datetime
from typing import List, Dict, Optional
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

        # Database
        self.__init_db__()
        self.cur_trade = self.read_last_opened_trade()

        self._log.info("Last trades:")
        self._log.info(self.db_session.query(Trade).order_by(Trade.open_time.desc()).limit(10).all())

        self._log.info(f"Current opened trade: {self.cur_trade}")
        self._log.info("Completed init broker")

    def __init_db__(self):
        # Create database
        db_path = "../data/biml.db"
        self._log.info(f"Init database, path: {db_path}")
        engine = create_engine(f"sqlite:///{db_path}")
        Trade.metadata.create_all(engine)
        self.db_session = sessionmaker(engine)()
        self.cur_trade = self.read_last_opened_trade()

    def create_trade(self, symbol: str, order_type: int,
                     quantity: float,
                     price: Optional[float],
                     stop_loss: Optional[float]) -> Optional[float]:
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
        order_id = res["orderId"]
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
                      open_time=datetime.now(), open_price=filled_price, open_order_id=order_id,
                      stop_loss_price=stop_loss, stop_loss_order_id=stop_loss_order_id,
                      quantity=quantity, )
        return self.cur_trade

    def close_opened_positions(self, ticker: str):
        if not self.client:
            return
        opened_quantity, opened_orders = self.get_opened_positions(ticker)
        if opened_orders:
            self._log.info("Cancelling opened orders")
            self.client.cancel_open_orders(symbol=ticker)
        if opened_quantity:
            # if we sold (-1) we should buy and vice versa
            side = "BUY" if opened_quantity < 0 else "SELL"
            self._log.info(f"Creating {-opened_quantity} {side} order to close existing positions")
            res = self.client.new_order(symbol=ticker, side=side, type="MARKET",
                                        quantity=abs(opened_quantity))
            self._log.info(res)

    def get_opened_positions(self, symbol: str) -> (float, List[Dict]):
        """
        Quantity of symbol we have in portfolio
        """
        orders, opened_quantity = self.client.get_open_orders(symbol), 0
        if orders:
            # Currently opened orders is trailing stop loss against main order
            last_order = orders[-1]
            opened_quantity = float(last_order["origQty"])
            # stoploss is buy => main order was sell
            if last_order["side"] == "BUY": opened_quantity *= -1.0
        self._log.info(f"We have {opened_quantity} {symbol} in portfolio and {len(orders)} opened orders for {symbol}")
        return opened_quantity, orders

    def read_last_opened_trade(self) -> Trade:
        """ Returns current opened trade, stored in db or none """
        return self.db_session \
            .query(Trade) \
            .where(Trade.close_time.is_(None)) \
            .order_by(Trade.open_time.desc()).first()

    def end_cur_trade(self, symbol: str) -> Trade:
        """
        If current opened trade exists,  close it, error otherwise
        """
        self._log.info(f"Close trade for {symbol}")
        assert self.cur_trade

        # Cancel stop loss order for current trade
        self.close_opened_positions(symbol)

        side = self.order_side_names[-self.cur_trade.side]
        # Place closing order for current trade
        res = self.client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=self.cur_trade.quantity
        )
        filled_price = float(res["fills"][0]["price"] if res["fills"] else None)
        order_id = res["orderId"]

        # Update stat db
        self.cur_trade.close_order_id = order_id
        self.cur_trade.close_time = datetime.now()
        self.cur_trade.close_price = filled_price

        self.db_session.flush()
        self.cur_trade, closed_trade = None, self.cur_trade
        return closed_trade

    def clear_cur_trade_if_closed(self):
        """ If cur trade closed by stop loss, update db and set cur trade variable to none """

        if not self.cur_trade:
            return
        # Get single closing trade or None
        close_trade = (self.client.my_trades(symbol=self.cur_trade.ticker, orderId=self.cur_trade.stop_loss_order_id)
                       or [None])[-1]
        if close_trade:
            self._log.debug(f"Current trade found closed by stop loss or take profit. "
                            f"stop_loss_order_id: {self.cur_trade.stop_loss_order_id}")
            # Update db
            self.cur_trade.close_order_id=self.cur_trade.stop_loss_order_id
            self.cur_trade.close_price = close_trade["price"]
            self.cur_trade.close_time = datetime.utcfromtimestamp(close_trade["time"] / 1000.0)
            self.db_session.flush([self.cur_trade])

            self.cur_trade = None


if __name__ == "__main__":
    def create_test_client():
        # todo: remove this
        from App import App
        config = App._load_config()
        key, secret, url = config["biml.connector.key"], config["biml.connector.secret"], config[
            "biml.connector.url"]
        print(f"Init binance client, url: {url}")
        return Client(key=key, secret=secret, base_url=url, timeout=10)


    client = create_test_client()
    orders = client.get_orders("BTCUSDT")
    order = client.get_order(symbol="BTCUSDT", orderId=orders[-1]["orderId"])
    # broker = BinanceBroker(client = client)
    # broker.cur_trade = Trade(ticker="BTCUSDT", close_order_id=)
    # broker.update_cur_trade()

    # detail=client.my_trades("BTCUSDT")
    # orders = client.get_orders("BTCUSDT")

    trades = client.my_trades("BTCUSDT", orderId="123")
    trades = client.my_trades("BTCUSDT")
    (client.my_trades("BTCUSDT", orderId="123") or [None])[-1]
    # print(orders)

    print("Done")
