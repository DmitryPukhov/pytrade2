import logging
from datetime import datetime
from io import StringIO
from typing import Dict, Optional

from binance.spot import Spot as Client

from exch.BrokerBase import BrokerBase
from model.Trade import Trade
from model.TradeStatus import TradeStatus


class BinanceBroker(BrokerBase):
    """ Trading functions for Binance """

    def __init__(self, client: Client, config: Dict[str, str]):
        self._log = logging.getLogger(self.__class__.__name__)
        self.client: Client = client
        super().__init__(config)

    def create_order(self, symbol: str, direction: int, price: float, quantity: float) -> Optional[Trade]:
        """ Make the order, return filled trade for the order"""

        # Make order
        side = Trade.order_side_names[direction]
        res = self.client.new_order(
            symbol=symbol,
            side=side,
            type="LIMIT",
            price=price,
            quantity=quantity,
            timeInForce="FOK"  # Fill or kill
        )
        self._log.debug(f"Create main order raw response: {res}")

        # Return the result
        trade: Optional[Trade] = None
        if res["status"] == "FILLED":
            trade = Trade(ticker=symbol,
                          side=side,
                          open_time=datetime.utcfromtimestamp(res["transactTime"] / 1000.0),
                          open_price=float(res["fills"][0]["price"] if res["fills"] else price),
                          open_order_id=str(res["orderId"]),
                          quantity=quantity)
        return trade

    def create_sl_tp_order(self, base_trade: Trade,
                           stop_loss_price: float,
                           stop_loss_limit_price: float,
                           take_profit_price: float) -> Trade:
        """ Stop loss + take profit order """
        sl_side = Trade.order_side_names[-base_trade.direction()]
        res = self.client.new_oco_order(
            symbol=base_trade.ticker,
            side=sl_side,
            quantity=base_trade.quantity,
            price=take_profit_price,
            stopPrice=stop_loss_price,
            stopLimitPrice=stop_loss_limit_price,
            stopLimitTimeInForce='GTC')
        self._log.debug(f"Stop loss / take profit order raw response: {res}")
        oco_res = self.client.get_oco_order(orderListId=str(res["orderListId"]))
        sl_tp_order_ids = ",".join([str(sl_tp_order["orderId"]) for sl_tp_order in oco_res["orders"]])
        base_trade.stop_loss_order_id = sl_tp_order_ids
        base_trade.stop_loss_price = stop_loss_price
        base_trade.take_profit_price = take_profit_price
        return base_trade

    def create_sl_order(self, base_trade: Trade,
                        stop_loss_price: float,
                        stop_loss_limit_price: float) -> Optional[str]:
        """ Stop loss order without take profit"""
        sl_side = Trade.order_side_names[-base_trade.direction()]
        res = self.client.new_order(
            symbol=base_trade.ticker,
            side=sl_side,
            quantity=base_trade.quantity,
            type="STOP_LOSS_LIMIT",
            stopPrice=stop_loss_price,  # When stopPrice is reached, place limit order with price.
            price=stop_loss_limit_price,  # Order executed with this price or better (ideally stopPrice)
            trailingDelta=200,  # 200 bips=2%
            timeInForce="GTC")
        self._log.debug(f"Stop loss order raw response: {res}")
        sl_order_id = str(res["orderId"])
        base_trade.stop_loss_order_id = sl_order_id
        base_trade.stop_loss_price = stop_loss_price
        return base_trade

    def create_closing_order(self, trade: Trade):
        base_direction = Trade.order_side_codes[trade.side]
        res = self.client.new_order(
            symbol=trade.ticker,
            side=Trade.order_side_names[-base_direction],
            type="MARKET",
            quantity=trade.quantity
        )
        self._log.debug(f"Closing order raw response: {res}")
        # Set current trade closure fields
        if res["status"] == "FILLED":
            trade.close_order_id = str(res["orderId"])
            trade.close_price = float(res["fills"][0]["price"])
            trade.close_time = datetime.utcfromtimestamp(res["transactTime"] / 1000.0)
        else:
            # Full stop, hanging order, requires urgent investigation
            raise f"Cannot create closing order for {trade}"
        return trade

    def update_cur_trade_status(self):
        """ If given trade closed by stop loss, update db and set cur trade variable to none """

        """ If given trade closed by stop loss, update db and set cur trade variable to none """
        if not self.cur_trade or not self.cur_trade.stop_loss_order_id:
            return

        # Try to get trade for stop loss or take profit
        for sltp_order_id in self.cur_trade.stop_loss_order_id.split(","):
            # Actually a single trade or empty list will be returned by my_trades
            for close_trade in self.client.my_trades(symbol=self.cur_trade.ticker, orderId=sltp_order_id):
                # Update db
                self.cur_trade.close_order_id = str(close_trade["orderId"])
                self.cur_trade.close_price = float(close_trade["price"])
                self.cur_trade.close_time = datetime.utcfromtimestamp(close_trade["time"] / 1000.0)
                self.cur_trade.status = TradeStatus.closed
                self.db_session.commit()

        if self.cur_trade.status == TradeStatus.closed:
                self.cur_trade = None

    def get_report(self):
        """ Short info for report """

        # Form message string
        msg = StringIO()
        msg.write(f"Allow trade: {self.allow_trade}\n")

        # Opened trade
        msg.write(f"Current trade: {self.cur_trade}\n")

        try:
            # Opened orders
            for order in self.client.get_open_orders():
                order_time = datetime.utcfromtimestamp(order["time"] / 1000.0)
                msg.write(f"Opened order: {order['side']} {order['symbol']}, id: {order['orderId']}, "
                          f"price: {order['price']}, time: {order_time}\n")
        except Exception as e:
            self._log.error(f"Error reporting opened orders: {e}")

        try:
            # Account balance
            for b in self.client.account()["balances"]:
                if float(b["free"]) > 0 or b["locked"] > 0:
                    msg.write(f"{b['asset']} free: {b['free']}, locked: {b['locked']}\n")
        except Exception as e:
            self._log.error(f"Error reporting account info: {e}")

        return msg.getvalue()
