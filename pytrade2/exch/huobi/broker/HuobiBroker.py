import logging
from datetime import datetime
from typing import Dict, Optional
import huobi.client.wallet
import pandas as pd
from binance.spot import Spot as Client
from huobi.constant import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model.Trade import Trade

from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
from huobi.client.algo import AlgoClient

from model.Trade import Trade


class HuobiBroker:
    """ Trading functions for Huobi """

    def __init__(self, config: Dict[str, str]):
        self._log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.client = self.__trade_client()

        self.account_id = int(config["pytrade2.broker.hiobi.account.id"])

    def __trade_client(self):
        """ Huobi trade client creation."""
        key = self.config["pytrade2.exchange.huobi.connector.key"]
        secret = self.config["pytrade2.exchange.huobi.connector.secret"]
        url = self.config["pytrade2.exchange.huobi.connector.url"]
        self._log.info(f"Init Huobi client, url: {url}, key: ***{key[-3:]}, secret: ***{secret[-3:]}")
        return TradeClient(url=url, api_key=key, api_secret=secret)

    def create_order(self, symbol: str, direction: int, price: float, quantity: float) -> Optional[Trade]:
        """ Make the order, return filled trade for the order"""
        # Calculate huobi order type
        if direction == 1:
            order_type = OrderType.BUY_LIMIT_FOK
        elif direction == -1:
            order_type = OrderType.SELL_LIMIT_FOK
        else:
            order_type = 0

        # Make order using trade client
        order_id = self.client.create_order(
            symbol=symbol,
            account_id=self.account_id,
            order_type=order_type,
            amount=quantity,
            price=price)

        # Make order using Algo client
        # order_id = self.client.create_order(
        #     symbol=symbol,
        #     order_size=quantity,
        #     order_side=Trade.order_side_names[direction],
        #     order_type=AlgoOrderType.LIMIT,
        #     account_id=self.account_id,
        #     order_value=quantity,
        #     order_price=price
        # )
        # Create trade to return
        if order_id:
            order = self.client.get_order(order_id)

            # Set cur trade to opened order with sl/tp
            trade = Trade(ticker=symbol,
                          side=Trade.order_side_names[direction],
                          open_time=pd.to_datetime(order.created_at, unit="ms"),
                          open_price=order.price,
                          open_order_id=order_id,
                          quantity=order.amount)
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
        if base_trade.direction == 1:
            sl_order_type = OrderType.SELL_STOP_LIMIT
        elif base_trade.direction == -1:
            sl_order_type = OrderType.BUY_STOP_LIMIT
        else:
            sl_order_type = 0
        # Trade client we stop loss???
        sl_tp_order_id = self.client.create_order(
            symbol=base_trade.ticker,
            account_id=self.account_id,
            order_type=sl_order_type,
            amount=base_trade.quantity,
            price=stop_loss_limit_price,
            stop_price=stop_loss_price)

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

        self._log.debug(f"Created sl/tp order, id: {sl_tp_order_id}")
        base_trade.stop_loss_order_id = sl_tp_order_id
        base_trade.stop_loss_price = stop_loss_price
        base_trade.take_profit_price = take_profit_price
        return base_trade

    def create_sl_order(self, base_trade: Trade,
                        stop_loss_price: float,
                        stop_loss_limit_price: float) -> Optional[str]:
        """ Stop loss order without take profit"""
        """ Stop loss + take profit order """
        # Calculate huobi order type
        if base_trade.direction == 1:
            sl_order_type = OrderType.SELL_STOP_LIMIT
        elif base_trade.direction == -1:
            sl_order_type = OrderType.BUY_STOP_LIMIT
        else:
            sl_order_type = 0
        # Trade client we stop loss???
        sl_tp_order_id = self.client.create_order(
            symbol=base_trade.ticker,
            account_id=self.account_id,
            order_type=sl_order_type,
            amount=base_trade.quantity,
            price=stop_loss_limit_price,
            stop_price=stop_loss_price)

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

        close_order_id = self.client.create_order(
            symbol=trade.ticker,
            account_id=self.account_id,
            order_type=close_order_type,
            amount=trade.quantity)

        if close_order_id:
            close_order = self.client.get_order(close_order_id)
            trade.close_order_id = close_order_id
            trade.close_price = close_order.price
            trade.close_time = datetime.utcfromtimestamp(close_order.finished_at / 1000.0)
        else:
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
            close_order = self.client.get_order(order_id=int(trade.stop_loss_order_id))
            if close_order.state == OrderState.FILLED:
                # Update db
                trade.close_order_id = sltp_order_id
                trade.close_price = close_order.price
                trade.close_time = datetime.utcfromtimestamp(close_order.finished_at / 1000.0)
        return trade
