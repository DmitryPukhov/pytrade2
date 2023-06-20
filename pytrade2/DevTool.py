##### For dev only #####
import datetime
import logging
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
        strategy = "LSTMStrategy"
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

    def print_balance(self, header: str):
        # Account balance
        msg = StringIO(header)
        balance = self.account_client.get_balance(account_id=self.account_id)
        actual_balance = "\n".join(
            [f"{b.currency} amount: {b.balance}, type: {b.type}" for b in balance if float(b.balance) > 0])
        msg.write(actual_balance)
        print(msg.getvalue())

    def test_rest_client(self):
        hc = HuobiRestClient(access_key=self.key, secret_key=self.secret)

        print(hc.get("/swap-api/v1/swap_api_trading_status"))
        print(hc.post("/swap-api/v1/swap_account_info"))
        # future
        # host = 'api.hbdm.vn'
        path = '/api/v1/contract_position_info'
        params = {'symbol': 'btc'}
        print('future:{}\n'.format(hc.post(path, params)))

        # coin-swap
        # host = 'api.hbdm.vn'
        path = '/swap-api/v1/swap_position_info'
        params = {'contract_code': 'btc-usd'}
        print('coin-swap:{}\n'.format(hc.post(path, params)))

        # usdt-swap
        # host = 'api.hbdm.vn'
        path = '/linear-swap-api/v1/swap_cross_position_info'
        params = {'contract_code': 'btc-usdt'}
        print('usdt-swap:{}\n'.format(hc.post(path, params)))

    def test_usdt(self):
        access_key, secret_key = self.key, self.secret

        ################# usdt-swap
        print('*****************\nstart usdt-swap ws.\n')
        # wss://api.hbdm.com/swap-ws
        host = 'api.hbdm.com'
        path = '/linear-swap-ws'
        with HuobiWebSocketClient(host, path, access_key, secret_key, False) as usdt_swap:
            # usdt_swap = HuobiWebSocketClient(host, path, access_key, secret_key, False)
            # usdt_swap.open()

            # sub depth: https://huobiapi.github.io/docs/coin_margined_swap/v1/en/#subscribe-market-depth-data
            # sub_params = {
            #     "sub": "market.BTC-USD.depth.step15"
            #     #"id": "123"
            # }
            # sub_params = {
            #     "sub": "market.BTC-USD.bbo"
            #     #"id": "123"
            # }
            sub_params = {
                "sub": "market.BTC-USDT.trade.detail"
                # "id": "123"
            }
            usdt_swap.sub(sub_params, object())
            time.sleep(100)
            # usdt_swap.close()
            print('end usdt-swap ws.\n')

    def test_ws_swap(self):
        access_key, secret_key = self.key, self.secret

        ################# usdt-swap
        print('*****************\nstart usdt-swap ws.\n')
        # wss://api.hbdm.com/swap-ws
        host = 'api.hbdm.com'
        path = '/swap-ws'
        with HuobiWebSocketClient(host, path, access_key, secret_key, False) as usdt_swap:
            # usdt_swap = HuobiWebSocketClient(host, path, access_key, secret_key, False)
            # usdt_swap.open()

            # sub depth: https://huobiapi.github.io/docs/coin_margined_swap/v1/en/#subscribe-market-depth-data
            # sub_params = {
            #     "sub": "market.BTC-USD.depth.step15"
            #     #"id": "123"
            # }
            # sub_params = {
            #     "sub": "market.BTC-USD.bbo"
            #     #"id": "123"
            # }
            sub_params = {
                "sub": "market.BTC-USD.trade.detail"
                # "id": "123"
            }
            usdt_swap.sub(sub_params, "")
            time.sleep(100)
            # usdt_swap.close()
            print('end usdt-swap ws.\n')

    def test_ws_client(self):
        access_key, secret_key = self.key, self.secret

        ################# spot
        print('*****************\nstart spot ws.\n')
        host = 'api.huobi.de.com'
        path = '/ws/v2'
        with HuobiWebSocketClient(host, path, access_key, secret_key, True) as spot:
            # only sub interface
            sub_params = {
                "action": "sub",
                "ch": "accounts.update"
            }
            spot.sub(sub_params, "")
            time.sleep(10)
            print('end spot ws.\n')

        ################# future
        print('*****************\nstart future ws.\n')
        host = 'api.hbdm.vn'
        path = '/notification'
        with HuobiWebSocketClient(host, path, access_key, secret_key, False) as future:
            # only sub interface
            sub_params = {
                "op": "sub",
                "topic": "accounts.trx"
            }
            future.sub(sub_params, "")
            time.sleep(10)
            print('end future ws.\n')

        ################# coin-swap
        print('*****************\nstart coin-swap ws.\n')
        host = 'api.hbdm.vn'
        path = '/swap-notification'
        with HuobiWebSocketClient(host, path, access_key, secret_key, False) as coin_swap:
            # only sub interface
            sub_params = {
                "op": "sub",
                "topic": "accounts.TRX-USD"
            }
            coin_swap.sub(sub_params, "")
            time.sleep(10)
            print('end coin-swap ws.\n')

        ################# usdt-swap
        print('*****************\nstart usdt-swap ws.\n')
        host = 'api.hbdm.vn'
        path = '/linear-swap-notification'
        with HuobiWebSocketClient(host, path, access_key, secret_key, False) as usdt_swap:
            # only sub interface
            sub_params = {
                "op": "sub",
                "topic": "accounts_cross.USDT"
            }
            usdt_swap.sub(sub_params, "")
            time.sleep(10)
            print('end usdt-swap ws.\n')

    def new_hbdm_broker(self):
        rc = HuobiRestClient(access_key=self.key, secret_key=self.secret)
        ws = HuobiWebSocketClient(host="api.hbdm.vn", path="'/linear-swap-notification", access_key=self.key,
                                  secret_key=self.secret, be_spot=False)
        return HuobiBrokerHbdm(conf=self.config, rest_client=rc, ws_client=ws)


if __name__ == "__main__":

    dt = DevTool()
    broker = dt.new_hbdm_broker()


    # Inquiry error. Please try again later
    # res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_relation_tpsl_order",
    #                             {"pair": "BTC-USDT", "order_id": 1120523720633106435})

    # res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_tpsl_openorders", {"pair": "BTC-USDT"});
    def is_my_order(order_id, source_order_id):
        return order_id in {'1120523720679243778,1120523720679243777'} or source_order_id == '1120523720633106435'

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

    def print_orders(res):
        orders = res["data"]
        print("orders:")
        for o in sorted(orders, key=lambda o: -o["update_time"]):
            uts = o["update_time"]
            udt = datetime.datetime.fromtimestamp(uts / 1000)
            cts = o["create_date"]
            cdt = datetime.datetime.fromtimestamp(cts / 1000)
            print(f"Order id: {o['order_id']}, direction: {o['direction']}, status: {o['status']}, created:{cdt},  updated: {udt}, is_tpsl: {o['is_tpsl']}, price: {o['trade_avg_price']}")

    #
    # open '1120523720633106435'
    # sl '1120523720679243778,1120523720679243777'
    #
    # closed at 21:53: 1120512647122935809, 1120512647122935808
    #res=broker.rest_client.post("/linear-swap-api/v1/swap_cross_order_info", {"pair": "BTC-USDT", "order_id": 1120523720633106435})

    dt = datetime.datetime(year=2023, month=6, day=19, hour=20, minute=21, second=13)
    ts = int(dt.timestamp() *1000)
    #res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_order_info", {"order_id": 1120512647106158594})
    res=broker.rest_client.post("/linear-swap-api/v3/swap_cross_hisorders", {"contract": "BTC-USDT", "trade_type":17,
                                                                             "type":2, "status": 6})
    print_orders(res)
    res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_tpsl_hisorders",
                                  {"pair": "BTC-USDT", "status": 0,  "create_date": ts})
    print_sltp_orders(res)

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
    # print(broker.rest_client.post("/linear-swap-api/v1/swap_cross_relation_tpsl_order",
    #                               {"contract_code": "BTC-USDT", "order_id": 1119997217854570496}))

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
