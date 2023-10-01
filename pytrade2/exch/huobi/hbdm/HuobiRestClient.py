import logging

import requests
import yaml
from urllib import parse
import json
from datetime import datetime
import hmac
import base64
from hashlib import sha256


class HuobiRestClient:
    """
    Client to make get/post requests to these Huobi rest services:
    https://huobiapi.github.io/docs/coin_margined_swap/v1/en/#introduction
    """

    def __init__(self, access_key: str, secret_key: str):
        
        self.access_key, self.secret_key = access_key, secret_key
        # Futures, coins url
        self.host = 'api.hbdm.vn'

    @staticmethod
    def _auth_params_of(method: str, access_key: str, secret_key: str, host: str, path: str) -> str:
        """ Fill authorization parameters in rest call url """

        # Format and url encode timestamp
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        timestamp = parse.quote(timestamp)

        suffix = f'AccessKeyId={access_key}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={timestamp}'
        payload = f'{method.upper()}\n{host}\n{path}\n{suffix}'

        digest = hmac.new(key=secret_key.encode('utf8'),
                          msg=payload.encode('utf8'),
                          digestmod=sha256).digest()  # make sha256 with binary data

        # base64 encode with binary data and then get string
        signature = base64.b64encode(digest).decode()
        signature = parse.quote(signature)  # url encode

        suffix = '{}&Signature={}'.format(suffix, signature)
        return suffix

    def get(self, path: str, params: dict = None) -> json:
        """ Make authorized GET request to given service with given parameters """
        try:
            # Compose url and headers
            url = f'https://{self.host}{path}?'
            logging.debug(f"Doing get request to url: {url}, params: {params}")
            url_suffix = self._auth_params_of('get', self.access_key, self.secret_key, self.host, path)
            url = url + url_suffix
            headers = {'Content-type': 'application/x-www-form-urlencoded'}
            # Request
            res_json = requests.get(url, params=params, headers=headers).json()
            logging.debug(f"Got response: {res_json}")
            return res_json
        except Exception as e:
            logging.error(e)
        return None

    def post(self, path: str, data: dict = None) -> json:
        """ Make authorized POST request to given service with given parameters """

        try:
            # Compose url and headers
            url = f'https://{self.host}{path}?'
            logging.debug(f"Doing post request to url: {url}, data: {data}")
            url_suffix = self._auth_params_of('post', self.access_key, self.secret_key, self.host, path)
            url = url + url_suffix
            # url = f'https://{self.host}{path}?{url_suffix}'
            headers = {'Accept': 'application/json', 'Content-type': 'application/json'}
            # Post request to huobi rest service
            res_json = requests.post(url, json=data, headers=headers).json()
            logging.debug(f"Got response: {res_json}")
            return res_json
        except Exception as e:
            logging.error(e)
        return None
