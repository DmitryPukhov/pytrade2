class MetricNames:
    class Prediction:
        pred_fut_low_diff = "pred_fut_low_diff"
        pred_fut_high_diff = "pred_fut_high_diff"
        pred_time = "pred_time"
        pred_cur_time = "pred_cur_time"

    class Signal:
        signal = "signal"
        signal_time = "signal_time"
        signal_price = "signal_price"
        signal_sl = "signal_sl"
        signal_tp = "signal_tp"
        signal_tr_delta = "signal_tr_delta"

    class Broker:
        class Account:
            balance = "balance"

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