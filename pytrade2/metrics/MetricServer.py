import logging
import threading
from functools import wraps

from flask import Flask, request, abort
from prometheus_client import make_wsgi_app

from metrics.Metrics import Metrics


class MetricServer:
    """Work with Prometheus metrics"""

    metrics:Metrics = None
    # token value to expect in header Authorization: Bearer <auth_token>
    auth_token = None
    #
    # # Metrics
    # gauges: Dict[str, Gauge] = {}
    # counters: Dict[str, Counter] = {}
    # summaries: Dict[str, Summary] = {}

    # Flask app to expose metrics endpoint
    app = Flask("pytrade2")
    prometheus_wsgi_app = make_wsgi_app()

    @staticmethod
    def require_api_token(func):
        """ Authorization to secure metrics endpoind. """

        @wraps(func)
        def check_token(*args, **kwargs):
            auth_token = request.headers.get("Authorization", "").lstrip("Bearer ")
            if (MetricServer.auth_token != auth_token):
                # Kick off who not authorized
                abort(401)

            # Otherwise just send them where they wanted to go
            return func(*args, **kwargs)

        return check_token

    @staticmethod
    @app.route("/metrics")
    @require_api_token
    def metrics():
        """ Flask endpoint for metrics"""
        return MetricServer.prometheus_wsgi_app

    @staticmethod
    def start_http_server():
        """ Start Flask thread to expose metrics to prometheus server."""
        threading.Thread(target=MetricServer.app.run, kwargs={"host": "0.0.0.0", "port": 5000}).start()
        logging.info("Prometheus started")

    # @classmethod
    # def name_of(cls, source, suffix):
    #     return f'pytrade2_{MetricServer.app_name.lower()}_{source.__class__.__name__.lower()}_{suffix}'
    #
    # @classmethod
    # def gauge(cls, source, suffix) -> Gauge:
    #     name = cls.name_of(source, suffix)
    #     if name not in cls.gauges:
    #         cls.gauges[name] = Gauge(name, name)
    #     return cls.gauges[name]
    #
    # @classmethod
    # def counter(cls, source, suffix) -> Counter:
    #     name = cls.name_of(source, suffix)
    #     if name not in cls.counters:
    #         cls.counters[name] = Counter(name, name)
    #     return cls.counters[name]
    #
    # @classmethod
    # def summary(cls, source, suffix) -> Summary:
    #     name = cls.name_of(source, suffix)
    #     if name not in cls.summaries:
    #         cls.summaries[name] = Summary(name, name)
    #     return cls.summaries[name]
