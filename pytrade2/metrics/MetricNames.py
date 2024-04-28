class MetricNames:
    class Prediction:
        pred_signal = "pred_signal"
        pred_fut_low_diff = "pred_fut_low_diff"
        pred_fut_high_diff = "pred_fut_high_diff"
        pred_time = "pred_time"
        pred_cur_time = "pred_cur_time"

    class Broker:
        class Account:
            balance = "balance"

        class Order:
            order_create_ok = "order_create_ok"
            order_create_not_filled = "order_create_not_filled"
            order_create_error = "order_create_error"
