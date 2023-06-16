##### For dev only #####
import logging
from io import StringIO

import requests
from urllib import parse
import json
from datetime import datetime
import hmac
import base64
from hashlib import sha256

import requests
import yaml
from huobi.client.account import AccountClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
import asyncio

from huobi.constant import OrderType, OrderSource
from websocket import WebSocket

from exch.huobi.HuobiRestClient import HuobiRestClient


class DevTool():
    """"" For dev purpose only, don't call or run from the app """

    def __init__(self):
        # Read config
        strategy = "SimpleKerasStrategy"
        cfgpath = f"../deploy/yandex_cloud/secret/{strategy.lower()}.yaml"
        with open(cfgpath, "r") as file:
            print(f"Reading config from {cfgpath}")
            cfg = yaml.safe_load(file)
        # Get keys from config
        key = cfg["pytrade2.exchange.huobi.connector.key"]
        secret = cfg["pytrade2.exchange.huobi.connector.secret"]

        self.trade_client = TradeClient(api_key=key, secret_key=secret)
        self.market_client = MarketClient(api_key=key, secret_key=secret)
        self.account_client = AccountClient(api_key=key, secret_key=secret)
        self.account_id = cfg["pytrade2.broker.huobi.account.id"]

    def print_balance(self, header: str):
        # Account balance
        msg = StringIO(header)
        balance = self.account_client.get_balance(account_id=self.account_id)
        actual_balance = "\n".join(
            [f"{b.currency} amount: {b.balance}, type: {b.type}" for b in balance if float(b.balance) > 0])
        msg.write(actual_balance)
        print(msg.getvalue())


if __name__ == "__main__":
    hc = HuobiRestClient()

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
