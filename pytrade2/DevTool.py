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


class HuobiRestClient:
    """
    Client to make get/post requests to these Huobi rest services:
    https://huobiapi.github.io/docs/coin_margined_swap/v1/en/#introduction
    """

    def __init__(self):
        # todo: rewrite to read normal config
        strategy = "SimpleKerasStrategy"
        cfgpath = f"../deploy/yandex_cloud/secret/{strategy.lower()}.yaml"
        with open(cfgpath, "r") as file:
            print(f"Reading config from {cfgpath}")
            cfg = yaml.safe_load(file)
        # Get keys from config
        self.access_key = cfg["pytrade2.exchange.huobi.connector.key"]
        self.secret_key = cfg["pytrade2.exchange.huobi.connector.secret"]
        # coin-swap, futures
        # self.host = "api.hbdm.com"
        self.host = 'api.hbdm.vn'

    def _get_url_suffix(self, method: str, access_key: str, secret_key: str, host: str, path: str) -> str:
        """ Fill authorization parameters in rest call url """

        # it's utc time and an example is 2017-05-11T15:19:30
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        timestamp = parse.quote(timestamp)  # url encode
        suffix = 'AccessKeyId={}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={}'.format(
            access_key, timestamp)
        payload = '{}\n{}\n{}\n{}'.format(method.upper(), host, path, suffix)

        digest = hmac.new(secret_key.encode('utf8'), payload.encode(
            'utf8'), digestmod=sha256).digest()  # make sha256 with binary data

        # base64 encode with binary data and then get string
        signature = base64.b64encode(digest).decode()
        signature = parse.quote(signature)  # url encode

        suffix = '{}&Signature={}'.format(suffix, signature)
        return suffix

    def get(self, path: str, params: dict = None) -> json:
        """ Make authorized GET request to given service with given parameters """
        try:
            url_suffix = self._get_url_suffix('get', self.access_key, self.secret_key, self.host, path)
            url = f'https://{self.host}{path}?{url_suffix}'
            headers = {'Content-type': 'application/x-www-form-urlencoded'}
            # params are parts of url path
            res = requests.get(url, params=params, headers=headers)
            data = res.json()
            return data
        except Exception as e:
            logging.error(e)
        return None

    def post(self, path: str, data: dict = None) -> json:
        """ Make authorized POST request to given service with given parameters """

        try:
            url_suffix = self._get_url_suffix('post', self.access_key, self.secret_key, self.host, path)
            url = f'https://{self.host}{path}?{url_suffix}'
            headers = {'Accept': 'application/json',
                       'Content-type': 'application/json'}
            # json means data with json format string in http body
            res = requests.post(url, json=data, headers=headers)
            data = res.json()
            return data
        except Exception as e:
            logging.error(e)
        return None


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
