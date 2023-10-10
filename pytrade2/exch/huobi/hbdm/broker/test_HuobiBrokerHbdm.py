import copy
import os
import sys
from datetime import datetime
from multiprocessing import RLock
from unittest import TestCase, skip

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
from BrokerMocks import BrokerMocks

from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm
from exch.huobi.hbdm.broker.OrderCreator import OrderCreator

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

    def test_update_trade_sl_buy(self):
        # sl/tp response example
        # Test buy trade
        trade = Trade()
        trade.side = "BUY"
        sltpres = copy.deepcopy(self.sltp_res)
        sltpres["data"]["tpsl_order_info"] = sltpres["data"]["tpsl_order_info"][:1]

        OrderCreator.update_trade_sltp(sltpres, trade)

        # Should fill stop loss only
        self.assertEqual("1119997217904902144", trade.stop_loss_order_id)
        self.assertEqual(27000.0, trade.stop_loss_price)
        self.assertIsNone(trade.take_profit_price)

    def test_update_trade_sltp_buy(self):
        # sl/tp response example
        # Test buy trade
        trade = Trade()
        trade.side = "BUY"
        OrderCreator.update_trade_sltp(self.sltp_res, trade)

        self.assertEqual("1119997217909096448,1119997217904902144", trade.stop_loss_order_id)
        self.assertEqual(26000.0, trade.stop_loss_price)
        self.assertEqual(27000.0, trade.take_profit_price)

    def test_update_trade_sltp_sell(self):
        # sl/tp response example
        # Test buy trade
        trade = Trade()
        trade.side = "SELL"
        OrderCreator.update_trade_sltp(self.sltp_res, trade)

        self.assertEqual("1119997217904902144,1119997217909096448", trade.stop_loss_order_id)
        self.assertEqual(26000.0, trade.take_profit_price)
        self.assertEqual(27000.0, trade.stop_loss_price)

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
        actual = OrderCreator.res2trade(res)

        self.assertEqual("BTC-USDT", actual.ticker)
        self.assertEqual("opened", actual.status)
        self.assertEqual(datetime.utcfromtimestamp(1687069745272 / 1000), actual.open_time)
        self.assertEqual(26542, actual.open_price)
        self.assertEqual("1119997217854570496", actual.open_order_id)

    def test_adjust_prices(self):
        price, sl_trigger, sl_order, tp_trigger, tp_order = OrderCreator.adjust_prices(1, 100, 90, 110, 2, 0.1)
        self.assertEqual(100.0, price)
        self.assertEqual(81.0, sl_order)
        self.assertEqual(110.0, tp_trigger)
        self.assertEqual(99.0, tp_order)  # 110*(1-0.1)

    def test_adjust_prices_precisions(self):
        price, _, _, _, _ = OrderCreator.adjust_prices(1, 100, 90, 110, 2, 0.1)
        self.assertEqual(100.0, price)

    def test_cur_trade_params__no_trailingstop(self):
        actual = OrderCreator.cur_trade_params(symbol="symbol1",
                                               client_order_id=1,
                                               side="side1",
                                               price=2,
                                               quantity=3,
                                               sl_trigger_price=4,
                                               sl_order_price=5,
                                               tp_trigger_price=6
                                               )
        expected = {"contract_code": "symbol1",
                    "client_order_id": 1,
                    # "contract_type": "swap",
                    "volume": 3,
                    "direction": "side1",
                    "price": 2,
                    "lever_rate": 1,
                    "order_price_type": "optimal_5_fok",
                    "reduce_only": 0,  # 0 for opening order
                    "sl_trigger_price": 4,
                    "sl_order_price": 5,
                    "sl_order_price_type": "limit",
                    # No trailing stop => tp set explicitly
                    "tp_trigger_price": 6,
                    "tp_order_price_type": "optimal_5"
                    }
        self.assertDictEqual(expected, actual)

    def test_cur_trade_params__trailingstop(self):
        actual = OrderCreator.cur_trade_params(symbol="symbol1",
                                               client_order_id=1,
                                               side="side1",
                                               price=2,
                                               quantity=3,
                                               sl_trigger_price=4,
                                               sl_order_price=5
                                               )
        expected = {"contract_code": "symbol1",
                    "client_order_id": 1,
                    # "contract_type": "swap",
                    "volume": 3,
                    "direction": "side1",
                    "price": 2,
                    "lever_rate": 1,
                    "order_price_type": "optimal_5_fok",
                    "reduce_only": 0,  # 0 for opening order
                    "sl_trigger_price": 4,
                    "sl_order_price": 5,
                    "sl_order_price_type": "limit"
                    }
        self.assertDictEqual(expected, actual)

    def test_on_socket_data(self):
        # Socket order message
        msg = {'contract_type': 'swap', 'pair': 'BTC-USDT', 'business_type': 'swap', 'op': 'notify',
               'topic': 'orders_cross.btc-usdt', 'ts': 1688816846369, 'symbol': 'BTC', 'contract_code': 'BTC-USDT',
               'volume': 1, 'price': 30462.5, 'order_price_type': 'limit', 'direction': 'buy', 'offset': 'both',
               'status': 6, 'lever_rate': 1, 'order_id': 1127325090859466752, 'order_id_str': '1127325090859466752',
               'client_order_id': 1127324473901215747, 'order_source': 'tpsl', 'order_type': 1,
               'created_at': 1688816846338, 'trade_volume': 1, 'trade_turnover': 30.1774, 'fee': -0.01207096,
               'trade_avg_price': 30177.4, 'margin_frozen': 0, 'profit': -0.0262, 'trade': [
                {'trade_fee': -0.01207096, 'fee_asset': 'USDT', 'real_profit': -0.0262, 'ht_price': None,
                 'profit': -0.0262, 'trade_id': 100012572201452, 'id': '100012572201452-1127325090859466752-1',
                 'trade_volume': 1, 'trade_price': 30177.4, 'trade_turnover': 30.1774, 'created_at': 1688816846351,
                 'role': 'taker'}], 'canceled_at': 0, 'fee_asset': 'USDT', 'margin_asset': 'USDT', 'uid': '450694896',
               'liquidation_type': '0', 'margin_mode': 'cross', 'margin_account': 'USDT', 'is_tpsl': 0,
               'real_profit': -0.0262, 'trade_partition': 'USDT', 'reduce_only': 1}
        # Current trade
        trade = Trade()
        trade.side = "SELL"

        broker: HuobiBrokerHbdm = BrokerMocks.broker_mock()
        broker.cur_trade = trade

        broker.on_socket_data("orders_cross.btc-usdt", msg)
        self.assertIsNone(broker.cur_trade)


