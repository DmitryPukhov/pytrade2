import websocket
import threading
import time
import json
import gzip
from datetime import datetime
from urllib import parse
import hmac
import base64
from hashlib import sha256


class HuobiWebSocketClient:
    """
    WebSocket Subscription Address
    Market Data Request and Subscription: wss://api.hbdm.com/swap-ws

    Order Push Subscription: wss://api.hbdm.com/swap-notification

    Index Kline Data and Basis Data Subscription: wss://api.hbdm.com/ws_index

    System status updates subscription ：wss://api.hbdm.com/center-notification

    If the url: api.hbdm.com can't be accessed, please use the url below:

    Market Data Request and Subscription Address: wss://api.btcgateway.pro/swap-ws;

    Order Push Subscription：wss://api.btcgateway.pro/swap-notification

    Index Kline Data and Basis Data Subscription: wss://api.btcgateway.pro/ws_index

    System status updates subscription ：wss://api.btcgateway.pro/center-notification
    """

    def __init__(self, host: str, path: str, access_key: str, secret_key: str, be_spot: bool):
        self._host = host
        self._path = path
        self._access_key = access_key
        self._secret_key = secret_key
        self._be_spot = be_spot
        self._active_close = False
        self._has_open = False
        self._sub_str = None
        self._ws = None
        self.consumers = []

    def __del__(self):
        self.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    def open(self):
        url = 'wss://{}{}'.format(self._host, self._path)
        self._ws = websocket.WebSocketApp(url,
                                          on_open=self._on_open,
                                          on_message=self._on_msg,
                                          on_close=self._on_close,
                                          on_error=self._on_error)
        t = threading.Thread(target=self._ws.run_forever, daemon=True)
        t.start()


    def _on_open(self, ws):
        print('ws open')
        signature_data = self._get_signature_data()  # signature data
        self._ws.send(json.dumps(signature_data))  # as json string to be send
        self._has_open = True
        for consumer in [c for c in self.consumers if hasattr(c, 'on_socket_open')]:
            consumer.on_socket_open()

    def _get_signature_data(self) -> dict:
        # it's utc time and an example is 2017-05-11T15:19:30
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        url_timestamp = parse.quote(timestamp)  # url encode

        # get Signature
        if self._be_spot:
            suffix = 'accessKey={}&signatureMethod=HmacSHA256&signatureVersion=2.1&timestamp={}'.format(
                self._access_key, url_timestamp)
        else:
            suffix = 'AccessKeyId={}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={}'.format(
                self._access_key, url_timestamp)
        payload = '{}\n{}\n{}\n{}'.format('GET', self._host, self._path, suffix)

        digest = hmac.new(self._secret_key.encode('utf8'), payload.encode(
            'utf8'), digestmod=sha256).digest()  # make sha256 with binary data
        # base64 encode with binary data and then get string
        signature = base64.b64encode(digest).decode()

        # data
        if self._be_spot:
            data = {
                "action": "req",
                "ch": "auth",
                "params": {
                    "authType": "api",
                    "accessKey": self._access_key,
                    "signatureMethod": "HmacSHA256",
                    "signatureVersion": "2.1",
                    "timestamp": timestamp,
                    "signature": signature
                }
            }
        else:
            data = {
                "op": "auth",
                "type": "api",
                "AccessKeyId": self._access_key,
                "SignatureMethod": "HmacSHA256",
                "SignatureVersion": "2",
                "Timestamp": timestamp,
                "Signature": signature
            }
        return data

    def _on_msg(self, ws, message):
        plain = message
        if not self._be_spot:
            plain = gzip.decompress(message).decode()

        jdata = json.loads(plain)
        if 'ping' in jdata:
            sdata = plain.replace('ping', 'pong')
            self._ws.send(sdata)
            return
        elif 'op' in jdata:
            opdata = jdata['op']
            if opdata == 'ping':
                sdata = plain.replace('ping', 'pong')
                self._ws.send(sdata)
                return
            else:
                pass
        elif 'action' in jdata:
            opdata = jdata['action']
            if opdata == 'ping':
                sdata = plain.replace('ping', 'pong')
                self._ws.send(sdata)
                return
            else:
                pass
        else:
            pass
        print(jdata)
        for consumer in self.consumers:
            consumer.on_socket_data(jdata)

    def _on_close(self, ws):
        print("ws close.")
        self._has_open = False
        if not self._active_close and self._sub_str is not None:
            self.open()
            self.sub(self._sub_str)

    def _on_error(self, ws, error):
        print(error)

    def sub(self, sub_str: dict):
        if self._active_close:
            print('has close')
            return
        while not self._has_open:
            time.sleep(1)

        self._sub_str = sub_str
        self._ws.send(json.dumps(sub_str))  # as json string to be send
        print(sub_str)

    def close(self):
        self._active_close = True
        self._sub_str = None
        self._has_open = False
        self._ws.close()
