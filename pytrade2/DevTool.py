##### For dev only #####
from io import StringIO

import time

import yaml
from huobi.client.account import AccountClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient

from exch.huobi.hbdm.HuobiRestClient import HuobiRestClient
from exch.huobi.hbdm.HuobiWebSocketClient import HuobiWebSocketClient
from exch.huobi.hbdm.broker.HuobiBrokerHbdm import HuobiBrokerHbdm


class DevTool():
    """"" For dev purpose only, don't call or run from the app """

    def __init__(self):
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

        self.trade_client = TradeClient(api_key=key, secret_key=secret)
        self.market_client = MarketClient(api_key=key, secret_key=secret)
        self.account_client = AccountClient(api_key=key, secret_key=secret)
        self.account_id = self.config["pytrade2.broker.huobi.account.id"]

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
            usdt_swap.sub(sub_params)
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
            usdt_swap.sub(sub_params)
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
            spot.sub(sub_params)
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
            future.sub(sub_params)
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
            coin_swap.sub(sub_params)
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
            usdt_swap.sub(sub_params)
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
    res = broker.rest_client.post("/linear-swap-api/v1/swap_cross_switch_position_mode", {"margin_account": "btc-usdt", "position_mode": "single_side"})
    # print(res)
    broker.create_cur_trade(symbol="BTC-USDT", direction=1, quantity=1, price=26520, stop_loss_price=26000,
                            take_profit_price=27000)


    #print(broker.rest_client.post("/linear-swap-api/v1/swap_cross_account_info", {"contract_code": "USDT"}))
    #dt.test_usdt()
    # dt.test_ws_swap()
