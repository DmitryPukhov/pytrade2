import json
import logging
import threading
from functools import wraps

from flask import Flask, request, abort
from prometheus_client import make_wsgi_app

from pytrade2.metrics.Metrics import Metrics


class MetricServer:
    """Work with Prometheus metrics"""

    # Metric names to refer from the app
    metrics: Metrics = None
    app_config: dict[str, any] = {}
    app_params: dict[str, any] = {}

    # token value to expect in header Authorization: Bearer <auth_token>
    auth_token = None
    app_name = "pytrade2"
    # Flask app to expose metrics endpoint
    app = Flask(app_name)
    prometheus_wsgi_app = make_wsgi_app()

    @staticmethod
    def require_api_token(func):
        """ Authorization to secure metrics endpoind. """
        return func
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
    @app.route("/app_config/query", methods=['GET', 'POST'])
    @app.route("/app_config/metrics", methods=['GET', 'POST'])
    @app.route("/app_config", methods=['GET', 'POST'])
    @require_api_token
    def get_app_config():
        """ Flask endpoint for configuration"""
        logging.debug(f"Got request: {str(request.args.to_dict())}")
        logging.debug(f"Will return app info: {MetricServer.app_config}")
        return json.dumps(MetricServer.app_config)

    @staticmethod
    @app.route("/app_params/query", methods=['GET', 'POST'])
    @app.route("/app_params/metrics", methods=['GET', 'POST'])
    @app.route("/app_params", methods=['GET', 'POST'])
    @require_api_token
    def get_app_params():
        """ Flask endpoint for configuration"""
        logging.debug(f"Got request: {str(request.args.to_dict())}")
        logging.debug(f"Will return app info: {MetricServer.app_params}")
        return json.dumps(MetricServer.app_params)

    @staticmethod
    def start_http_server():
        """ Start Flask thread to expose metrics to prometheus server."""
        threading.Thread(target=MetricServer.app.run, kwargs={"host": "0.0.0.0", "port": 8000}).start()
        logging.info("Prometheus started")
