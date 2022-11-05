from broker.BinanceBroker import BinanceBroker


class StrategyBase:

    def __init__(self, broker: BinanceBroker):
        # Binance spot client
        self.order_quantity = 0.001
        self.broker = broker
