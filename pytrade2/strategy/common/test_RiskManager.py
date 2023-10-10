import datetime
from unittest import TestCase
from unittest.mock import MagicMock

import pandas as pd

from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm
from model.Trade import Trade
from strategy.common.RiskManager import RiskManager


class TestRiskManager(TestCase):
    def broker_of(self, fee: float, last_close_time: datetime.datetime, last_direction, last_open_price: float,
                  last_close_price: float)->HuobiBrokerHbdm:
        broker_stub = MagicMock()
        broker_stub.fee = fee
        trade = Trade()
        trade.side = Trade.order_side_names[last_direction]
        trade.open_price = last_open_price
        trade.close_price = last_close_price
        trade.close_time = last_close_time
        broker_stub.prev_trade = trade
        return broker_stub

    def risk_manager_of(self, fee: float,
                        deny_window: pd.Timedelta,
                        last_close_time: datetime.datetime,
                        last_direction,
                        last_open_price: float,
                        last_close_price: float)->RiskManager:
        broker = self.broker_of(fee=fee,
                                           last_close_time=last_close_time,
                                           last_direction=last_direction,
                                           last_open_price=last_open_price,
                                           last_close_price=last_close_price)

        return RiskManager(broker, deny_window)

    def test_can_trade__allow_when_last_buy_after_deny_window(self):
        rm = self.risk_manager_of(fee=0.01,
                                    deny_window=pd.Timedelta("1min"),
                                    last_close_time=datetime.datetime(2023, 7, 4, 10, 18),
                                    last_direction=1,
                                    last_open_price=100.0,
                                    # Profit is negative
                                    last_close_price=99.0)
        # After deny window
        cur_time = rm._broker.prev_trade.close_time + pd.Timedelta("1m")

        self.assertTrue(rm.can_trade(cur_time=cur_time))

    def test_can_trade__deny_when_loss_buy_inside_deny_window(self):
        rm = self.risk_manager_of(fee=0.01,
                                  deny_window=pd.Timedelta("1min"),
                                  last_close_time=datetime.datetime(2023, 7, 4, 10, 18),
                                  last_direction=1,
                                  last_open_price=100.0,
                                  # Profit is negative
                                  last_close_price=100.019)
        cur_time = rm._broker.prev_trade.close_time + pd.Timedelta("59s")
        self.assertFalse(rm.can_trade(cur_time=cur_time))

    def test_can_trade__allow_when_profit_buy_inside_deny_window(self):
        rm = self.risk_manager_of(fee=0.01,
                                  deny_window=pd.Timedelta("1min"),
                                  last_close_time=datetime.datetime(2023, 7, 4, 10, 18),
                                  last_direction=1,
                                  last_open_price=100.0,
                                  # Profit is negative
                                  last_close_price=103)
        cur_time = rm._broker.prev_trade.close_time + pd.Timedelta("59s")
        self.assertTrue(rm.can_trade(cur_time=cur_time))

    def test_can_trade__allow_when_profit_buy_after_deny_window(self):
        rm = self.risk_manager_of(fee=0.01,
                                  deny_window=pd.Timedelta("1min"),
                                  last_close_time=datetime.datetime(2023, 7, 4, 10, 18),
                                  last_direction=1,
                                  last_open_price=100.0,
                                  # Profit is negative
                                  last_close_price=103)
        cur_time = rm._broker.prev_trade.close_time + pd.Timedelta("60s")
        self.assertTrue(rm.can_trade(cur_time=cur_time))

    def test_can_trade__allow_when_no_last_trade(self):
        rm = self.risk_manager_of(fee=0.01,
                                  deny_window=pd.Timedelta("1min"),
                                  # Last trade does not matter, will be set to None for this test
                                  last_close_time=datetime.datetime(2023, 7, 4, 10, 18),
                                  last_direction=1,
                                  last_open_price=100.0,
                                  # Profit is negative
                                  last_close_price=103)
        rm._broker.prev_trade = None
        cur_time = datetime.datetime(2023, 7, 4, 10, 18)
        self.assertTrue(rm.can_trade(cur_time=cur_time))

