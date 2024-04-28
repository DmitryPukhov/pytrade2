from prometheus_client import Gauge

class Metrics:

    class Strategy:
        def __init__(self, app_name: str, strategy: str):
            self.learn = Metrics.Strategy.Learn(app_name, strategy)
            self.prediction = Metrics.Strategy.Prediction(app_name, strategy)
            self.signal = Metrics.Strategy.Signal(app_name, strategy)

        class Learn:
            def __init__(self, app_name: str, strategy: str):
                self.train_period_sec = Gauge("strategy_learn_train_period_sec",
                                              "History amount to use for train", namespace=app_name, subsystem=strategy)
                self.train_exec_duration_sec = Gauge("strategy_learn_train_exec_duration_sec",
                                                     "Model fit() process duration", namespace=app_name,
                                                     subsystem=strategy)

        class Prediction:
            def __init__(self, app_name: str, strategy: str):
                self.pred_fut_low_diff = Gauge("strategy_pred_fut_low_diff", "pred_fut_low_diff", namespace=app_name,
                                               subsystem=strategy)
                self.pred_fut_high_diff = Gauge("strategy_pred_fut_high_diff", "pred_fut_high_diff", namespace=app_name,
                                                subsystem=strategy)
                # pred_time = "pred_time"
                self.pred_cur_time = Gauge("strategy_pred_cur_time", "pred_cur_time", namespace=app_name,
                                           subsystem=strategy)

        class Signal:
            def __init__(self, app_name: str, strategy: str):
                self.signal = Gauge("strategy_signal", "strategy_signal", namespace=app_name, subsystem=strategy)
                # signal_time = "strategy_signal_time"
                self.signal_price = Gauge("strategy_signal_price", "strategy_signal_price", namespace=app_name,
                                          subsystem=strategy)
                self.signal_sl = Gauge("strategy_signal_sl", "strategy_signal_sl", namespace=app_name,
                                       subsystem=strategy)
                self.signal_tp = Gauge("strategy_signal_tp", "strategy_signal_tp", namespace=app_name,
                                       subsystem=strategy)
                self.signal_tr_delta = Gauge("strategy_signal_tr_delta", "strategy_signal_tr_delta", namespace=app_name,
                                             subsystem=strategy)

    class Broker:
        def __init__(self, namespace: str, strategy: str):
            self.account = Metrics.Broker.Account(namespace, strategy)
            self.order = Metrics.Broker.Order(namespace, strategy)
            self.trade = Metrics.Broker.Trade(namespace, strategy)

        class Account:
            def __init__(self, app_name: str, strategy: str):
                self.balance = Gauge("broker_account_balance", "account_balance", namespace=app_name,
                                     subsystem=strategy)

        class Order:
            def __init__(self, app_name: str, strategy: str):
                self.order_create_ok = Gauge("broker_order_create_ok", "order_create_ok", namespace=app_name,
                                             subsystem=strategy)
                self.order_create_not_filled = Gauge("broker_order_create_not_filled", "order_create_not_filled",
                                                     namespace=app_name, subsystem=strategy)
                self.order_create_error = Gauge("broker_order_create_error", "order_create_error", namespace=app_name,
                                                subsystem=strategy)

        class Trade:
            def __init__(self, app_name: str, strategy: str):
                self.trade_open_price = Gauge("broker_trade_open_price", "trade_open_price", namespace=app_name,
                                              subsystem=strategy)
                self.trade_close_price = Gauge("broker_trade_close_price", "trade_close_price", namespace=app_name,
                                               subsystem=strategy)
                self.trade_sl = Gauge("broker_trade_sl", "trade_sl", namespace=app_name, subsystem=strategy)
                self.trade_tp = Gauge("broker_trade_tp", "trade_tp", namespace=app_name, subsystem=strategy)
                self.trade_tr_delta = Gauge("broker_trade_tr_delta", "trade_tr_delta", namespace=app_name,
                                            subsystem=strategy)
    #
    def __init__(self, app_name: str, strategy_name: str):
        self.strategy = Metrics.Strategy(app_name, strategy_name)
        self.broker = Metrics.Broker(app_name, strategy_name)
