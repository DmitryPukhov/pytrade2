from datetime import datetime, timezone, timedelta
from unittest import TestCase

import pandas as pd

from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm
from model.Trade import Trade


class TestOrderManager(TestCase):

    def test_update_trade_opened_event(self):
        dt = datetime(year=2023, month=6, day=12, hour=13, minute=16, second=1, tzinfo=timezone.utc)
        msg = {"order_id": 1, "trade_avg_price": 2, "created_at": int(dt.timestamp() * 1000)}
        trade = Trade()
        HuobiBrokerHbdm.update_trade_opened_event(msg, trade)

        self.assertEqual("opened", trade.status)
        self.assertEqual(2, trade.open_price)

    def test_update_trade_closed_event(self):
        dt = datetime(year=2023, month=6, day=12, hour=13, minute=16, second=1, tzinfo=timezone.utc)
        msg = {"order_id": 1, "trade_avg_price": 2, "created_at": int(dt.timestamp() * 1000)}
        trade = Trade()
        HuobiBrokerHbdm.update_trade_closed_event(msg, trade)

        self.assertEqual("closed", trade.status)
        self.assertEqual("1", trade.close_order_id)
        self.assertEqual(datetime(year=2023, month=6, day=12, hour=13, minute=16, second=1), trade.close_time)

    def test_update_trade_closed(self):
        raw = {'direction': 'sell', 'offset': 'both', 'volume': 1.0, 'price': 26583.0, 'profit': 0.05,
               'pair': 'BTC-USDT',
               'query_id': 69592538249, 'order_id': 1120016247351635968, 'contract_code': 'BTC-USDT', 'symbol': 'BTC',
               'lever_rate': 1, 'create_date': 1687074282253, 'order_source': 'web', 'canceled_source': '',
               'order_price_type': 4, 'order_type': 1, 'margin_frozen': 0.0, 'trade_volume': 1.0,
               'trade_turnover': 26.592, 'fee': -0.0106368, 'trade_avg_price': 26592.0, 'status': 6,
               'order_id_str': '1120016247351635968', 'fee_asset': 'USDT', 'fee_amount': 0,
               'fee_quote_amount': 0.0106368,
               'liquidation_type': '0', 'margin_asset': 'USDT', 'margin_mode': 'cross', 'margin_account': 'USDT',
               'update_time': 1687074282782, 'is_tpsl': 0, 'real_profit': 0.05, 'trade_partition': 'USDT',
               'reduce_only': 1, 'contract_type': 'swap', 'business_type': 'swap'}
        trade = Trade()
        HuobiBrokerHbdm.update_trade_closed(raw, trade)

        self.assertEqual("1120016247351635968", trade.close_order_id)
        self.assertEqual(26592.0, trade.close_price)
        self.assertEqual(datetime.utcfromtimestamp(1687074282782 / 1000), trade.close_time)
        self.assertEqual("closed", trade.status)

    def test_huobi_history_close_order_query_params(self):
        dt = pd.Timestamp(year=2023, month=6, day=18, hour=11, minute=28, second=1)
        trade = Trade()
        trade.open_time = dt
        trade.side = "BUY"
        actual = HuobiBrokerHbdm.huobi_history_close_order_query_params(trade)
        self.assertEqual(18, actual["trade_type"])  # sell
        self.assertEqual(2, actual["type"])  # finished
        expected_ts = (dt-timedelta(hours=1)).timestamp()*1000
        self.assertEqual(expected_ts, actual["start_time"])
