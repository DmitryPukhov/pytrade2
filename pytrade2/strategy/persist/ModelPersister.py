import functools
import glob
import logging
import os
import threading
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict
import boto3
import pandas as pd
from keras.models import Model
import threading

from lightgbm import LGBMRegressor
from sklearn.multioutput import MultiOutputRegressor
import lightgbm as lgb
import pickle

class ModelPersister:
    """ Read/write model weights"""

    def __init__(self, config: Dict, tag: str):

        # Directory for model weights and price data
        self.data_dir = config["pytrade2.data.dir"]
        if self.data_dir:
            # weights dir
            self.model_dir = str(Path(self.data_dir, tag, "model"))
            Path(self.model_dir).mkdir(parents=True, exist_ok=True)

    def load_last_model(self, model: Model):
        try:
            saved_models = glob.glob(str(Path(self.model_dir, "*.*")))
            if not saved_models:
                logging.info(f"No saved models in {self.model_dir}")
                return model

            if isinstance(model, Model):
                saved_models = glob.glob(str(Path(self.model_dir, "*.index")))
                # Load keras
                last_model_path = str(sorted(saved_models)[-1])[:-len(".index")]
                logging.info(f"Load keras model from {last_model_path}")
                model.load_weights(last_model_path)
            else:
                last_model_path = str(sorted(saved_models)[-1])
                logging.info(f"Load lgb model from {last_model_path}")
                with open(last_model_path, 'rb') as f:
                    model = pickle.load(f)

        except Exception as e:
            logging.warning(f'Error loading last model. It\'s ok if the model architecture is changed. Error: {e}')

        return model

    def save_model(self, model):
        model_path = str(Path(self.model_dir, datetime.utcnow().isoformat()))
        if isinstance(model, Model):
            # Save keras
            logging.debug(f"Save keras model to {model_path}")
            model.save_weights(model_path)

        elif isinstance(model, MultiOutputRegressor) and isinstance(model.estimator, LGBMRegressor):
            # Save lgb
            model_path += "_lgb.pkl"
            with open(model_path,'wb') as f:
                pickle.dump(model,f)
                logging.debug(f"Saved lgb  model to {model_path}")

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
            logging.debug(f"Purged {len(purge_files)} files in {self.model_dir}")
