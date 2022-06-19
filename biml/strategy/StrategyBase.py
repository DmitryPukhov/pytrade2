import logging
from binance.spot import Spot as Client


class StrategyBase:

    def __init__(self, client: Client):
        # Binance spot client
        self.client: Client = client

    def is_out_of_market(self, symbol: str):
        """
        Assert if we are out of trade for this symbol: total buy and sell counts should be equal
        Raise exception if not
        """
        logging.debug("Checking opened orders")
        trades = self.client.my_trades(symbol)
        logging.info(trades)
        # Sum of quantities for buy and sell trades should be 0. Otherwize we have unclosed trades.
        opened_quantity = sum([float(trade["qty"]) * (1 if trade["isBuyer"] else -1) for trade in trades])
        return opened_quantity == 0

    def create_order(self, symbol: str, side: str, price: float, quantity: float, stop_loss_ratio: float,
                     ticker_size: int = 2):
        """
        Buy or sell order with trailing stop loss
        """
        trailing_delta = int(100 * stop_loss_ratio * 100)  # trailing delta in points

        stop_loss_price = price
        if side == "BUY":
            stop_loss_price = price * (1 - stop_loss_ratio)
        elif side == "SELL":
            stop_loss_price = price * (1 + stop_loss_ratio)
        stop_loss_price = round(stop_loss_price, ticker_size)
        logging.info(f"Creating {side} order and trailing stop loss order, symbol={symbol}, price={price},"
                     f" stop_loss_price={stop_loss_price}, trailing_delta={trailing_delta}")

        # Main order
        res = self.client.new_order_test(
            symbol=symbol,
            side=side,
            type="LIMIT",
            price=price,
            quantity=quantity,
            timeInForce="GTC")
        logging.info(res)

        # Trailing stop loss order
        res = self.client.new_order_test(
            symbol=symbol,
            side=side,
            type='STOP_LOSS_LIMIT',
            quantity=quantity,
            price=stop_loss_price,
            stopPrice=price,
            trailingDelta=trailing_delta,
            timeInForce="GTC")
        logging.info(res)
