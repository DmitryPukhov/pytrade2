class MetricNames:
    class Strategy:
        class Learn:
            train_period = "strategy_learn_train_period"
            test_period = "strategy_learn_train_period"

        class Prediction:
            pred_fut_low_diff = "pred_fut_low_diff"
            pred_fut_high_diff = "pred_fut_high_diff"
            pred_time = "pred_time"
            pred_cur_time = "pred_cur_time"

        class Signal:
            signal = "strategy_signal"
            signal_time = "strategy_signal_time"
            signal_price = "strategy_signal_price"
            signal_sl = "strategy_signal_sl"
            signal_tp = "strategy_signal_tp"
            signal_tr_delta = "strategy_signal_tr_delta"

    class Broker:
        class Account:
            balance = "account_balance"

        class Order:
            order_create_ok = "order_create_ok"
            order_create_not_filled = "order_create_not_filled"
            order_create_error = "order_create_error"

        class Trade:
            trade_open_price = "trade_open_price"
            trade_close_price = "trade_close_price"
            trade_sl = "trade_sl"
            trade_tp = "trade_tp"
            trade_tr_delta = "trade_tr_delta"