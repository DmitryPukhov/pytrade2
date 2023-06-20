##### For dev only #####
import datetime
import logging
import os
from io import StringIO

import time

import yaml
from huobi.client.account import AccountClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from exch.huobi.hbdm.broker.AccountManagerHbdm import AccountManagerHbdm
from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm


class DevTool():
    """"" For dev purpose only, don't call or run from the app """

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG)
        AccountManagerHbdm.sub_events = None
        HuobiBrokerHbdm.sub_events = None

        # Read config
        strategy = "SimpleKerasStrategy"
        yccfgdir = "../deploy/yandex_cloud/secret"
        devcfgdir = "../pytrade2/cfg"
        cfgpaths = [f"{devcfgdir}/app-defaults.yaml", f"{yccfgdir}/{strategy.lower()}.yaml"]
        self.config = {}
        for cfgpath in cfgpaths:
            with open(cfgpath, "r") as file:
                print(f"Reading config from {cfgpath}")
                curcfg = yaml.safe_load(file)
                self.config.update(curcfg)
        # Get keys from config
        key = self.config["pytrade2.exchange.huobi.connector.key"]
        secret = self.config["pytrade2.exchange.huobi.connector.secret"]
        print(f"Key: {key[-3:]}, secret: {secret[-3:]}")

        AccountManagerHbdm.sub_events = None
        HuobiBrokerHbdm.sub_events = None
        self.key, self.secret = key, secret

    def new_hbdm_broker(self):
        rc = HuobiRestClient(access_key=self.key, secret_key=self.secret)
        ws = HuobiWebSocketClient(host="api.hbdm.vn", path="'/linear-swap-notification", access_key=self.key,
                                  secret_key=self.secret, be_spot=False)
        return HuobiBrokerHbdm(conf=self.config, rest_client=rc, ws_client=ws)

    @staticmethod
    def print_sltp_orders(res):
        orders = res["data"]["orders"]
        print("sltp orders:")
        for o in sorted(orders, key=lambda o: -o["update_time"]):
            uts = o["update_time"]
            udt = datetime.datetime.fromtimestamp(uts / 1000)
            cts = o["created_at"]
            cdt = datetime.datetime.fromtimestamp(cts / 1000)
            print(f"Order id: {o['order_id']}, source id: {o['source_order_id']}, direction: {o['direction']}, "
                  f"status: {o['status']}, created:{cdt},  updated: {udt}, relation_order_id: {o['relation_order_id']}")

    @staticmethod
    def print_orders(res):
        orders = res["data"]
        print("orders:")
        for o in sorted(orders, key=lambda o: -o["update_time"]):
            uts = o["update_time"]
            udt = datetime.datetime.fromtimestamp(uts / 1000)
            cts = o["create_date"]
            cdt = datetime.datetime.fromtimestamp(cts / 1000)
            print(
                f"Order id: {o['order_id']}, direction: {o['direction']}, status: {o['status']}, created:{cdt},  updated: {udt}, is_tpsl: {o['is_tpsl']}, price: {o['trade_avg_price']}")


if __name__ == "__main__":
    os.environ['TZ'] = 'UTC'
    time.tzset()
    dt = DevTool()
    broker = dt.new_hbdm_broker()

    def is_my_order(order_id, source_order_id):
        return order_id in {'1120523720679243778,1120523720679243777'} or source_order_id == '1120523720633106435'



    #
    # open '1120523720633106435'
    # sl '1120523720679243778,1120523720679243777'
    #
    # closed at 21:53: 1120512647122935809, 1120512647122935808
    # res=broker.rest_client.post("/linear-swap-api/v1/swap_cross_order_info", {"pair": "BTC-USDT", "order_id": 1120523720633106435})

    # ???
    # ? Open order Order id: 1120709961248485376, direction: buy, status: 6, created:2023-06-20 05:41:16.536000,  updated: 2023-06-20 05:41:18.695000, is_tpsl: 1, price: 26871.2
    # ? Closing order Order id: 1120706425254203392, direction: sell, status: 6, created:2023-06-20 05:27:13.492000,  updated: 2023-06-20 12:07:41.780000, is_tpsl: 1, price: 26938.7

    dt = datetime.datetime(year=2023, month=6, day=20, hour=2, minute=41, second=00)
    ts = int(dt.timestamp() * 1000)
    # res = broker.rest_client.post("/linear-swap-api/v3/swap_cross_hisorders",
    #                               {'contract': 'BTC-USDT', 'trade_type': 18, 'type': 2, 'status': 6,
    #                                'start_time': 1687239676536})
    # type2 - finished,
    #
    # trade_type 0:All; 1: Open long; 2: Open short; 3: Close short; 4: Close long;
    # 5: Liquidate long positions; 6: Liquidate short positions, 17:buy(one-way mode), 18:sell(one-way mode)

    res = broker.rest_client.post("/linear-swap-api/v3/swap_cross_hisorders",
                                  {'contract': 'BTC-USDT', 'trade_type': 18, 'type': 2, 'status': 6, "start_time":ts})

    DevTool.print_orders(res)
    # res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_relation_tpsl_order",
    #                               {"pair": "BTC-USDT", "order_id": 1120709961248485376})
    # DevTool.print_sltp_orders(res)

    # Works only for main order, not for sl/tp
    # res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_order_info", {"order_id": 1120523720679243777})
    # Error: the interface is offline
    # res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_hisorders", {"pair": "BTC-USDT", "create_date": ts, "status":0});
    # print(res)
    #
    # def is_my_order(order_id, source_order_id):
    #     return order_id in {'1120523720679243778,1120523720679243777'} or source_order_id == '1120523720633106435'
    #
    # my_orders = [o for o in res["data"]["orders"] if is_my_order(o["order_id"], o["source_order_id"])]
    # print(my_orders)
    # print(broker.get_report())
    # Set one way ok
    # res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_switch_position_mode", {"margin_account": "btc-usdt", "position_mode": "single_side"})
    # Create order ok
    # broker.create_cur_trade(symbol="BTC-USDT", direction=1, quantity=1, price=26720, stop_loss_price=26000,
    #                         take_profit_price=27000)

    # Request trade state - general setting, not orders
    # print(broker.rest_client.get("/linear-swap-api/v1/swap_cross_trade_state", {"contract_code": "BTC-USDT"}))

    # Order + sltp
    # Ok get order info by client id from broker
    # res = broker.get_order_info(client_order_id=1687058944, ticker="BTC-USDT")
    # print(f"Got {len(res['data'])} orders: {res}")

    # Opened sl/tp orders by main order id
    print(broker.rest_client.post("/linear-swap-api/v1/swap_cross_relation_tpsl_order",
                                  {"contract_code": "BTC-USDT", "order_id": 1120709961248485376}))

    # Get opened orders - do I need this?
    # print(broker.rest_client.post("/linear-swap-api/v1/swap_cross_trigger_openorders",  {"contract_code": "BTC-USDT"}))

    # res=broker.rest_client.post("/linear-swap-api/v3/swap_cross_hisorders", {"contract": "BTC-USDT", "trade_type":18, "type":2, "status": 6, "start_time":1687069745272})
    # print(res)
    # print(f"Got {len(res['data'])} orders")
    # for order in res["data"]:
    #     print(f"Order: {order}")
    # print(f"Got {len(res['data'])} orders")

    # print(broker.rest_client.post("/linear-swap-api/v1/swap_cross_account_info", {"contract_code": "USDT"}))
    # dt.test_usdt()
    # dt.test_ws_swap()
