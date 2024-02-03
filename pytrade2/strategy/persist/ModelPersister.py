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


class ModelPersister:
    """ Read/write model weights"""

    def __init__(self, config: Dict, tag: str):

        # Directory for model weights and price data
        self.data_dir = config["pytrade2.data.dir"]
        if self.data_dir:
            # weights dir
            self.model_weights_dir = str(Path(self.data_dir, tag, "weights"))
            Path(self.model_weights_dir).mkdir(parents=True, exist_ok=True)

    def load_last_model(self, model: Model):
        saved_models = glob.glob(str(Path(self.model_weights_dir, "*.index")))
        if saved_models:
            try:
                last_model_path = str(sorted(saved_models)[-1])[:-len(".index")]
                logging.info(f"Load model from {last_model_path}")
                model.load_weights(last_model_path)
            except Exception as e:
                logging.warning(f'Error loading last model. It\'s ok if the model architecture is changed. Error: {e}')
                pass
        else:
            logging.info(f"No saved models in {self.model_weights_dir}")

    def save_model(self, model):

        model_path = str(Path(self.model_weights_dir, datetime.utcnow().isoformat()))
        logging.debug(f"Save model to {model_path}")
        model.save_weights(model_path)

        self.purge_weights()

    def purge_weights(self, keep_count=1):
        """
        Purge old weights
        """
        keep_files_count = keep_count * 2 + 1  # .data, .index for each weight and one checkpoint file
        files = os.listdir(self.model_weights_dir)
        if files:
            purge_files = sorted(files, reverse=True)[keep_files_count:]
            for file in purge_files:
                os.remove(os.path.join(self.model_weights_dir, file))
            logging.debug(f"Purged {len(purge_files)} files in {self.model_weights_dir}")
