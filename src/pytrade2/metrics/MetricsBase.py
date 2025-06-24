import logging
import os
import time

from prometheus_client import CollectorRegistry, push_to_gateway


class MetricsBase:
    """
    Metrics base class. Provides a method to push metrics to a Prometheus pushgateway.
    """
    _logger = logging.getLogger("MetricsBase")
    gateway = os.getenv('PROMETHEUS_PUSHGATEWAY_URL', 'http://localhost:9091')
    strategy = os.getenv("pytrade2.strategy", "unknown_strategy")
    _push_to_gateway_interval_sec = float(os.environ.get('METRICS_PUSH_TO_GATEWAY_INTERVAL_SEC') or 5)
    registry = CollectorRegistry()
    run_flag = True

    @classmethod
    def push_to_gateway_(cls):
        try:
            if  cls._logger.isEnabledFor(logging.DEBUG):
                collected_metrics = cls.registry.collect()
                cls._logger.debug(f"Pushing {len(list(collected_metrics))} metrics to gateway {cls.gateway}")
            push_to_gateway(cls.gateway, job='trade_bots_farm', registry=cls.registry)
        except Exception as e:
            logging.error(f"Error while pushing metrics to {cls.gateway}: {e}")

    @classmethod
    def push_to_gateway_periodical(cls):
        cls._logger.info(
            f"Starting metrics pusher with a period of {cls._push_to_gateway_interval_sec} seconds. Gateway: {cls.gateway}")
        while cls.run_flag:
            try:
                # Push metrics to the Prometheus pushgateway.
                cls.push_to_gateway_()

                # Delay before the next push.
                time.sleep(cls._push_to_gateway_interval_sec)
            except Exception as e:
                logging.error(f"Error while pushing metrics to {cls.gateway}: {e}")
        cls._logger.info("Metrics pusher stopped.")
