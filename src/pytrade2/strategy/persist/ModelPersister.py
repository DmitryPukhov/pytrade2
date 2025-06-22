import glob
import logging
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Dict

import mlflow.sklearn
# from keras.models import Model
from lightgbm import LGBMRegressor
from mlflow import MlflowClient
from sklearn.multioutput import MultiOutputRegressor


class ModelPersister:
    """ Read/write model locally or read model from mlflow"""

    def __init__(self, config: Dict, tag: str):
        self._logger = logging.getLogger(self.__class__.__name__)

        # Directory for model weights and price data
        self.data_dir = config["pytrade2.data.dir"]
        if self.data_dir:
            # weights dir
            self.model_dir = str(Path(self.data_dir, tag, "model"))
            Path(self.model_dir).mkdir(parents=True, exist_ok=True)
        self.mlflow_client = MlflowClient()

    def is_keras_model(self, obj):
        return (obj.__class__.__name__ in {'Model', 'Sequential'} or
                hasattr(obj, '_keras_api_names'))

    def load_last_model(self, model=None):
        try:
            saved_models = glob.glob(str(Path(self.model_dir, "*.*")))
            if not saved_models:
                self._logger.info(f"No saved models in {self.model_dir}")
                return model

            if self.is_keras_model(model):
                from keras.models import Model
                saved_models = glob.glob(str(Path(self.model_dir, "*.index")))
                # Load keras
                last_model_path = str(sorted(saved_models)[-1])[:-len(".index")]
                self._logger.info(f"Load keras model from {last_model_path}")
                model.load_weights(last_model_path)
            else:
                last_model_path = str(sorted(saved_models)[-1])
                self._logger.info(f"Load lgb model from {last_model_path}")
                with open(last_model_path, 'rb') as f:
                    model = pickle.load(f)

        except Exception as e:
            self._logger.warning(f'Error loading last model. It\'s ok if the model architecture is changed. Error: {e}')

        return model

    def save_model(self, model):
        model_path = str(Path(self.model_dir, datetime.utcnow().isoformat()))
        # if isinstance(model, Model):
        if self.is_keras_model(model):
            # Save keras

            self._logger.debug(f"Save keras model to {model_path}")
            model.save_weights(model_path)

        elif isinstance(model, MultiOutputRegressor) and isinstance(model.estimator, LGBMRegressor):
            # Save lgb
            model_path += "_lgb.pkl"
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
                self._logger.debug(f"Saved lgb  model to {model_path}")

        self.purge_old_local_models()

    def purge_old_local_models(self, keep_count=1):
        """
        Purge old weights
        """
        keep_files_count = keep_count * 2 + 1  # .data, .index for each weight and one checkpoint file
        files = os.listdir(self.model_dir)
        if files:
            purge_files = sorted(files, reverse=True)[keep_files_count:]
            for file in purge_files:
                os.remove(os.path.join(self.model_dir, file))
            self._logger.debug(f"Purged {len(purge_files)} files in {self.model_dir}")

    def get_last_mlflow_trade_ready_model(self, model_name, load_func=mlflow.sklearn.load_model) -> (any, any, dict, any, any):
        # Model itself
        model, model_version, model_params = self._get_last_mlflow_trade_ready_model_like(model_name, load_func)
        # X, y pipeline if exist
        x_pipe_name = model_name + "_x_pipe"
        x_pipe, _, _ = self.get_last_mlflow_trade_ready_model(x_pipe_name, mlflow.sklearn.load_model)
        y_pipe, _, _ = self.get_last_mlflow_trade_ready_model(x_pipe_name, mlflow.sklearn.load_model)
        return model, model_version, model_params, x_pipe, y_pipe

    def _get_last_mlflow_trade_ready_model_like(self, model_name, load_func=mlflow.sklearn.load_model) -> (
    any, any, dict):
        """ Load latest model it's params and maybe pipelines from mlflow. The model should be tagged trade_ready.
        :return model, mlflow ModelVersion, params"""
        model, model_version, params = None, None, None
        try:
            trade_ready_tag = "is_trade_ready"
            self._logger.debug(f"Getting latest trade ready model: {model_name} from {self.mlflow_client.tracking_uri}")
            # Get last trade ready model version, tagged as trade ready
            model_versions = self.mlflow_client.search_model_versions(
                f"name = '{model_name}' and tag.{trade_ready_tag} = 'True'",
                order_by=["version_number desc"], max_results=1)
            if model_versions:
                model_version = model_versions.pop()
                self._logger.debug(f"Got model source: {model_version.source}")
                model = load_func(model_version.source)
                self._logger.debug(f"Loaded model")

                # Get run parameters
                params = self.mlflow_client.get_run(model_version.run_id).data.params
                self._logger.debug(f"Got strategy parameters: {params}")
            else:
                self._logger.warning(f"Model: {model_name} not found")
        except Exception as e:
            self._logger.error(e)

        return model, model_version, params
