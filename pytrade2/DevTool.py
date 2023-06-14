##### For dev only #####
from io import StringIO

import requests
import yaml
from huobi.client.account import AccountClient
from huobi.client.market import MarketClient
from huobi.client.trade import TradeClient
import asyncio

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


if __name__ == "__main__":

    res= requests.get(url="https://api.hbdm.com/swap-ex/market/depth?contract_code=BTC-USD&type=step5")

    wssurl="wss://api.hbdm.com/swap-ws"
    ws=WebSocket()
    ws.connect(url=wssurl)
    print(ws.getstatus())
    ws.close()




    # orderid1 = tool.trade_client.create_order(
    #     symbol="btcusdt",
    #     account_id=tool.account_id,
    #     order_type=OrderType.SELL_MARKET,
    #     amount=0.0012, # For sell in USD, for buy in btc
    #     price=0,
    #     source=OrderSource.API)
    # print(f"Order1 id: {orderid1}")
    #
    # tool.print_balance("After")
