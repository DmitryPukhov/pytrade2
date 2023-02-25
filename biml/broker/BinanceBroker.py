import logging
from typing import List, Dict, Optional
from binance.spot import Spot as Client
from broker.PairOrdersBroker import PairOrdersBroker


class BinanceBroker(PairOrdersBroker):
    """
    Orders management: buy, sell etc
    """

    def __init__(self, client: Client):
        super().__init__()
        self._log = logging.getLogger(self.__class__.__name__)
        self.client: Client = client
        self.order_sides = {1: "BUY", -1: "SELL"}

        self._log.info(f"Current opened trade: {self.cur_trade}")
        self._log.info("Completed init broker")

    def create_order(self, symbol: str, order_type: int,
                     quantity: float,
                     price: Optional[float],
                     stop_loss: Optional[float]) -> float:
        """
        Buy or sell with take profit and stop loss
        Binance does not support that in single order, so make 2 orders: main and stoploss/takeprofit
        """

        if not order_type:
            return
        ticker_size = 2

        side = self.order_sides[order_type]
        self._log.info(f"Creating order. Asset:{symbol}  side:{side}, price: {price}  quantity: {quantity}, stop loss: {stop_loss}")
        # Main buy or sell order with stop loss
        # This works
        res = self.client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=quantity
            )
        filled_price = float(res["fills"][0]["price"] if res["fills"] else price)
        if not filled_price:
            raise Exception(f"New order filled_price is empty: {res}")

        # stop loss
        if price and stop_loss:
            other_side = self.order_sides[-order_type]
            stop_loss = round(stop_loss, ticker_size)
            stop_loss_limit_price=stop_loss-(price-stop_loss)
            self._log.info(f"Creating stop loss order, stop_loss={stop_loss}, stop_loss_limit_price={stop_loss_limit_price}")
            res = self.client.new_order(
                symbol=symbol,
                side=other_side,
                type="STOP_LOSS_LIMIT",
                stopPrice=stop_loss, # When stopPrice is reached, place limit order with price.
                price=stop_loss_limit_price, # Order executed with this price or better (ideally stopPrice)
                trailingDelta=200, # 200 bips=2%
                timeInForce="GTC",
                quantity=quantity)
            self._log.debug(f"Stop loss order response: {res}")

        return filled_price

    def close_opened_positions(self, ticker: str):
        if not self.client:
            return
        opened_quantity, opened_orders = self.get_opened_positions(ticker)
        if opened_orders:
            self._log.info("Cancelling opened orders")
            self.client.cancel_open_orders(ticker)
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


if __name__ == "__main__":
    # todo: remove this
    from App import App
    config=App._load_config()
    key, secret, url = config["biml.connector.key"], config["biml.connector.secret"], config[
        "biml.connector.url"]
    print(f"Init binance client, url: {url}")
    client: Client = Client(key=key, secret=secret, base_url=url, timeout=10)
    orders=client.get_orders("BTCUSDT")
    trades=client.my_trades("BTCUSDT")
    print(orders)
    #broker=BinanceBroker(client=client)
    #broker.create_order(symbol="BTCUSDT",order_type=1, quantity=0.001, price=22900, stop_loss=21900)
    # res = client.new_order_test(
    #     symbol="BTCUSDT",
    #     side="sell",
    #     type="STOP_LOSS",
    #     price=22000.0,
    #     stopPrice=
    #     trailingDelta=200,
    #     timeInForce="GTC",
    #     #trailing_delta=200, # 200 bips=2%
    #     quantity=0.001)
    #print(res)

    print("Done")
