import logging
from collections import defaultdict
from typing import Dict, Set

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

    def __init__(self, host: str, path: str, access_key: str, secret_key: str, be_spot: bool, is_broker: bool):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._host = host
        self._path = path
        self.url = 'wss://{}{}'.format(self._host, self._path)
        self._access_key = access_key
        self._secret_key = secret_key
        self._be_spot = be_spot
        self._is_broker = is_broker
        self._active_close = False
        self._is_opening = False
        self.is_opened = False
        self._ws = None
        self._consumers = defaultdict(set)
        self._logger.info(f"Initialized, key: {access_key[-3:]}, secret: {secret_key[-3:]}")

    def __del__(self):
        self.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    def open(self):
        if self._is_opening or self.is_opened: return

        self._is_opening = True
        self._logger.info(f"Opening socket: {self.url}")
        self._ws = websocket.WebSocketApp(self.url,
                                          on_open=self._on_open,
                                          on_message=self._on_msg,
                                          on_close=self._on_close,
                                          on_error=self._on_error)
        t = threading.Thread(target=self._ws.run_forever, daemon=True)
        t.start()

    def _on_open(self, ws):
        self._logger.info(f"Socket opened: {self.url}")
        if self._is_broker:
            # Some endpoints requires this signature data, others just returns invalid command error and continue to work.
            signature_data = self._get_signature_data()  # signature data
            self._ws.send(json.dumps(signature_data))  # as json string to be send
        self.is_opened = True
        self._is_opening = False

        # Subscribe to messages for consumers
        for topic_consumers in self._consumers.values():
            for params, consumer in topic_consumers:
                self._logger.info(f"Subscribing to socket data, params: {params}, consumer: {consumer}")
                self._ws.send(params)  # as json string to be send

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
        try:
            plain = message
            if not self._be_spot:
                plain = gzip.decompress(message).decode()

            jdata = json.loads(plain)
            if 'ping' in jdata:
                sdata = plain.replace('ping', 'pong')
                self._ws.send(sdata)
                return
            elif 'op' in jdata:
                # Order and accounts notifications like {op: "notify", topic: "orders_cross@btc-usdt", data: []}
                opdata = jdata['op']
                if opdata == 'notify' and 'topic' in jdata:
                    # Pass the event to subscribers: broker, account, feed
                    topic = jdata['topic'].lower()
                    for params, consumer in [(params, consumer) for (params, consumer) in self._consumers[topic]
                                             if hasattr(consumer, 'on_socket_data')]:
                        consumer.on_socket_data(topic, jdata)
                elif opdata == 'ping':
                    sdata = plain.replace('ping', 'pong')
                    self._ws.send(sdata)
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
            elif 'ch' in jdata:
                # Pass the event to subscribers: broker, account, feed
                topic = jdata['ch'].lower()
                for params, consumer in [(params, consumer) for (params, consumer) in self._consumers[topic]
                                         if hasattr(consumer, 'on_socket_data')]:
                    consumer.on_socket_data(topic, jdata)
            elif jdata.get('status') == 'error':
                self._logger.error(f"Got message with error: {jdata}")
        except Exception as e:
            self._logger.error(e)

    def _on_close(self, ws):
        self._logger.info("Socket closed")
        self.is_opened = False
        if not self._active_close:
            self.open()

    def _on_error(self, ws, error):
        self._logger.error(f"Socket error: {error}")
        if not (self._is_opening or self.is_opened):
            self.open()


    def add_consumer(self, topic, params: dict, consumer):
        """ Registering consumer for the topic """
        self._logger.debug(f"Adding consumer, topic: {topic}, params: {params}, consumer: {consumer}")
        # topic -> (params, consumer obj)
        self._consumers[topic].add((json.dumps(params), consumer))

    def close(self):
        self._logger.info("Closing socket")
        self._active_close = True
        self.is_opened = False
        if self._ws: self._ws.close()
