from unittest import TestCase
from datetime import datetime, timezone

import pandas as pd

from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm
from model.Trade import Trade


class TestHuobiBrokerHbdm(TestCase):
    sltp_res = {'status': 'ok', 'data':
        {'contract_type': 'swap', 'business_type': 'swap', 'pair': 'BTC-USDT', 'symbol': 'BTC',
         'contract_code': 'BTC-USDT', 'margin_mode': 'cross', 'margin_account': 'USDT', 'volume': 1, 'price': 26720,
         'order_price_type': 'limit', 'direction': 'buy', 'offset': 'both', 'lever_rate': 1,
         'order_id': 1119997217854570496, 'order_id_str': '1119997217854570496', 'client_order_id': 1687058944,
         'created_at': 1687069745272, 'trade_volume': 1, 'trade_turnover': 26.542, 'fee': -0.0106168,
         'trade_avg_price': 26542.0, 'margin_frozen': 0, 'profit': 0, 'status': 6, 'order_type': 1,
         'order_source': 'api', 'fee_asset': 'USDT', 'canceled_at': 0, 'tpsl_order_info': [
            # sltp upper bound
            {'volume': 1.0, 'direction': 'sell', 'tpsl_order_type': 'tp', 'order_id': 1119997217904902144,
             'order_id_str': '1119997217904902144', 'trigger_type': 'ge', 'trigger_price': 27000.0,
             'order_price': 27270.0, 'created_at': 1687069745290, 'order_price_type': 'limit',
             'relation_tpsl_order_id': '1119997217909096448', 'status': 2, 'canceled_at': 0, 'fail_code': None,
             'fail_reason': None, 'triggered_price': None, 'relation_order_id': '-1'},
            # sltp lower bound
            {'volume': 1.0, 'direction': 'sell', 'tpsl_order_type': 'sl', 'order_id': 1119997217909096448,
             'order_id_str': '1119997217909096448', 'trigger_type': 'le', 'trigger_price': 26000.0,
             'order_price': 25740.0, 'created_at': 1687069745291, 'order_price_type': 'limit',
             'relation_tpsl_order_id': '1119997217904902144', 'status': 2, 'canceled_at': 0, 'fail_code': None,
             'fail_reason': None, 'triggered_price': None, 'relation_order_id': '-1'}],
         'trade_partition': 'USDT'}, 'ts': 1687071763277}

    def test_update_trade_opened(self):
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

    def test_update_trade_sltp_buy(self):
        # sl/tp response example
        # Test buy trade
        trade = Trade()
        trade.side = "BUY"
        HuobiBrokerHbdm.update_trade_sltp(self.sltp_res, trade)

        self.assertEqual("1119997217909096448,1119997217904902144", trade.stop_loss_order_id)
        self.assertEqual(25740.0, trade.stop_loss_price)
        self.assertEqual(27270.0, trade.take_profit_price)

    def test_update_trade_sltp_sell(self):
        # sl/tp response example
        # Test buy trade
        trade = Trade()
        trade.side = "SELL"
        HuobiBrokerHbdm.update_trade_sltp(self.sltp_res, trade)

        self.assertEqual("1119997217904902144,1119997217909096448", trade.stop_loss_order_id)
        self.assertEqual(25740.0, trade.take_profit_price)
        self.assertEqual(27270.0, trade.stop_loss_price)

    def test_update_trade_closed(self):
        res = {'code': 200, 'msg': 'ok', 'data': [
            {'direction': 'sell', 'offset': 'both', 'volume': 1.0, 'price': 26583.0, 'profit': 0.05, 'pair': 'BTC-USDT',
             'query_id': 69592538249, 'order_id': 1120016247351635968, 'contract_code': 'BTC-USDT', 'symbol': 'BTC',
             'lever_rate': 1, 'create_date': 1687074282253, 'order_source': 'web', 'canceled_source': '',
             'order_price_type': 4, 'order_type': 1, 'margin_frozen': 0.0, 'trade_volume': 1.0,
             'trade_turnover': 26.592, 'fee': -0.0106368, 'trade_avg_price': 26592.0, 'status': 6,
             'order_id_str': '1120016247351635968', 'fee_asset': 'USDT', 'fee_amount': 0, 'fee_quote_amount': 0.0106368,
             'liquidation_type': '0', 'margin_asset': 'USDT', 'margin_mode': 'cross', 'margin_account': 'USDT',
             'update_time': 1687074282782, 'is_tpsl': 0, 'real_profit': 0.05, 'trade_partition': 'USDT',
             'reduce_only': 1, 'contract_type': 'swap', 'business_type': 'swap'}], 'ts': 1687077630615}
        trade = Trade()
        HuobiBrokerHbdm.update_trade_closed(res, trade)

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
        self.assertEqual(dt.timestamp() * 1000, actual["start_time"])

    def test_res2trade(self):
        # Get order response
        res = {'status': 'ok', 'data': [
            {'business_type': 'swap', 'contract_type': 'swap', 'pair': 'BTC-USDT', 'remark2': None, 'symbol': 'BTC',
             'contract_code': 'BTC-USDT', 'volume': 1, 'price': 26720, 'order_price_type': 'limit', 'order_type': 1,
             'direction': 'buy', 'offset': 'both', 'lever_rate': 1, 'order_id': 1119997217854570496,
             'client_order_id': 1687058944, 'created_at': 1687069745272, 'trade_volume': 1, 'trade_turnover': 26.542,
             'fee': -0.0106168, 'trade_avg_price': 26542.0, 'margin_frozen': 0, 'profit': 0, 'status': 6,
             'order_source': 'api', 'canceled_source': None, 'order_id_str': '1119997217854570496', 'fee_asset': 'USDT',
             'fee_amount': 0, 'fee_quote_amount': 0.0106168, 'liquidation_type': '0', 'canceled_at': 0,
             'margin_asset': 'USDT', 'margin_account': 'USDT', 'margin_mode': 'cross', 'is_tpsl': 1, 'real_profit': 0,
             'trade_partition': 'USDT', 'reduce_only': 0}], 'ts': 1687069745771}
        actual = HuobiBrokerHbdm.res2trade(res)

        self.assertEqual("BTC-USDT", actual.ticker)
        self.assertEqual("opened", actual.status)
        self.assertEqual(datetime.utcfromtimestamp(1687069745272 / 1000), actual.open_time)
        self.assertEqual(26542, actual.open_price)
        self.assertEqual("1119997217854570496", actual.open_order_id)
