##### For dev only #####
import datetime
import logging
import os
from io import StringIO

import time
from threading import Thread, Timer
from unittest.mock import MagicMock

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
        strategy = "KerasLowHighClassificationStrategy"
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
        Timer.start = MagicMock()
        # ws = HuobiWebSocketClient(host="api.hbdm.vn",
        #                           path="'/linear-swap-notification",
        #                           access_key=self.key,
        #                           secret_key=self.secret,
        #                           is_broker=True,
        #                           be_spot=False)
        return HuobiBrokerHbdm(conf=self.config, rest_client=rc, ws_client=MagicMock(), ws_feed=MagicMock())

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

    @staticmethod
    def get_last_lowhigh():
        # Open order
        res = broker.rest_client.get("/linear-swap-ex/market/history/kline",
                                     {"contract_code": "BTC-USDT",
                                      "period": "1min",
                                      "size": 1})
        lastcandle = res["data"][-1]
        return lastcandle["low"], lastcandle["high"]


if __name__ == "__main__":
    os.environ['TZ'] = 'UTC'
    time.tzset()
    dt = DevTool()
    broker = dt.new_hbdm_broker()
    res = broker.get_sltp_orders_info(broker.cur_trade.open_order_id)
    # DevTool.print_sltp_orders(res)


    # Open
    low, high = DevTool.get_last_lowhigh()
    # logging.debug(f"Last low:{low}, high:{high}")
    broker.create_cur_trade(symbol="BTC-USDT",
                            direction=1,
                            quantity=1,
                            price=None,
                            stop_loss_price=low-1000,
                            take_profit_price=None,
                            trailing_delta=100)
    #
    # # Move stoploss
    # newsl = low - 200
    # broker.move_ts(newsl)
    # res = broker.get_sltp_orders_info(broker.cur_trade.open_order_id)
    # print(res)


