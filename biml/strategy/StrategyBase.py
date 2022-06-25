import logging
from typing import Dict, List

from binance.spot import Spot as Client


class StrategyBase:

    def __init__(self, client: Client):
        # Binance spot client
        self.client: Client = client

    def close_opened_positions(self, ticker: str):
        if not self.client:
            return
        opened_quantity, opened_orders = self.opened_positions(ticker)
        if opened_orders:
            logging.info("Cancelling opened orders")
            self.client.cancel_open_orders(ticker)
        if opened_quantity:
            # if we sold (-1) we should buy and vice versa
            side = "BUY" if opened_quantity < 0 else "SELL"
            logging.info(f"Creating {-opened_quantity} order to close existing positions")
            res=self.client.new_order(symbol=ticker, side="BUY" if opened_quantity < 0 else "SELL", type="MARKET",
                                  quantity=abs(opened_quantity))
            logging.info(res)

    def assert_out_of_market(self, ticker: str):
        """
        Raise exception if we have opened positions for the symbol
        """
        if self.client:
            opened_quantity, opened_orders = self.opened_positions(ticker)
            if opened_quantity or opened_orders:
                raise AssertionError(
                    f"Fatal: cannot trade. We have opened positions: {opened_quantity} {ticker} and {len(opened_orders)} opened {ticker} orders.")

    def opened_positions(self, symbol: str) -> (float, List[Dict]):
        """
        Quantity of symbol we have in portfolio
        """
        logging.debug("Checking opened orders")
        trades = self.client.my_trades(symbol)
        # Sum of quantities for buy and sell trades should be 0. Otherwize we have unclosed trades.
        opened_quantity = sum([float(trade["qty"]) * (1 if trade["isBuyer"] else -1) for trade in trades])
        logging.info(f"We have {opened_quantity} {symbol} in portfolio")

        orders = self.client.get_open_orders(symbol)
        # [{'symbol': 'BTCUSDT', 'orderId': 6104154, 'orderListId': -1, 'clientOrderId': 'Rwcdh0uW8Ocux22TXmpFmD', 'price': '21910.94000000', 'origQty': '0.00100000', 'executedQty': '0.00000000', 'cummulativeQuoteQty': '0.00000000', 'status': 'NEW', 'timeInForce': 'GTC', 'type': 'STOP_LOSS_LIMIT', 'side': 'SELL', 'stopPrice': '21481.31000000', 'trailingDelta': 200, 'icebergQty': '0.00000000', 'time': 1656149854953, 'updateTime': 1656159716195, 'isWorking': True, 'origQuoteOrderQty': '0.00000000'}]
        logging.info(f"We have {opened_quantity} {symbol} in portfolio and {len(orders)} opened orders for {symbol}")
        return opened_quantity, orders

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
