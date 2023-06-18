from unittest import TestCase
from datetime import datetime
from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm


class TestHuobiBrokerHbdm(TestCase):
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
        self.assertEqual(datetime.utcfromtimestamp(1687069745272/1000), actual.open_time)
        self.assertEqual(26542, actual.open_price)
        self.assertEqual("1119997217854570496", actual.open_order_id)

