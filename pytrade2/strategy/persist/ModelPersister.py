import glob
import logging
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Dict

import mlflow.sklearn
from keras.models import Model
from lightgbm import LGBMRegressor
from sklearn.multioutput import MultiOutputRegressor

from mlflow import MlflowClient


class ModelPersister:
    """ Read/write model weights"""

    def __init__(self, config: Dict, tag: str):
        self._logger = logging.getLogger(self.__class__.__name__)

        # Directory for model weights and price data
        self.data_dir = config["pytrade2.data.dir"]
        if self.data_dir:
            # weights dir
            self.model_dir = str(Path(self.data_dir, tag, "model"))
            Path(self.model_dir).mkdir(parents=True, exist_ok=True)
        self.mlflow_client = MlflowClient()

    def load_last_model(self, model: Model):
        try:
            saved_models = glob.glob(str(Path(self.model_dir, "*.*")))
            if not saved_models:
                self._logger.info(f"No saved models in {self.model_dir}")
                return model

            if isinstance(model, Model):
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
        if isinstance(model, Model):
            # Save keras
            self._logger.debug(f"Save keras model to {model_path}")
            model.save_weights(model_path)

        elif isinstance(model, MultiOutputRegressor) and isinstance(model.estimator, LGBMRegressor):
            # Save lgb
            model_path += "_lgb.pkl"
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
                self._logger.debug(f"Saved lgb  model to {model_path}")

        self.purge_old_models()

    def purge_old_models(self, keep_count=1):
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

    def get_last_trade_ready_model(self, model_name, load_func=mlflow.sklearn.load_model) -> (any, dict):
        """ Load latest model and it's params from mlflow. The model should be tagged trade_ready. """
        trade_ready_tag = "trade_ready"
        self._logger.info(f"Getting latest trade ready model: {model_name} from {self.mlflow_client.tracking_uri}")
        # Get last trade ready model version, tagged as trade ready
        model_versions = self.mlflow_client.search_model_versions(
            f"name = '{model_name}' and tag.{trade_ready_tag} = '1'",
            order_by=["version_number desc"], max_results=1)
        if not model_versions:
            self._logger.info(f"Model: {model_name} not found")
            return None, None
        model_version = model_versions.pop()
        self._logger.info(f"Got model: {model_version.source}")
        model = load_func(model_version.source)

        # Get run parameters
        params = self.mlflow_client.get_run(model_version.run_id).data.params
        self._logger.info(f"Got strategy parameters: {params}")

        return model, params
