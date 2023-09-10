import glob
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import pandas as pd
from keras.models import Model


class PersistableStateStrategy:
    """ Strategy whicn can save the data and read/write model weights."""

    def __init__(self, config: Dict):
        self._log = logging.getLogger(self.__class__.__name__)

        # Directory for model weights and price data
        self.data_dir = config["pytrade2.data.dir"]
        if self.data_dir:
            self.model_weights_dir = str(Path(self.data_dir, self.__class__.__name__, "weights"))
            Path(self.model_weights_dir).mkdir(parents=True, exist_ok=True)
            self.model_Xy_dir = str(Path(self.data_dir, self.__class__.__name__, "Xy"))
            Path(self.model_Xy_dir).mkdir(parents=True, exist_ok=True)
        self.last_save_time = datetime.utcnow()
        self.model = None
        # Save data each 10 seconds
        self.save_interval: timedelta = timedelta(seconds=60)
        self.X_buf = pd.DataFrame()
        self.y_buf = pd.DataFrame()
        self.data_bufs: Dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)

    def load_last_model(self, model: Model):
        saved_models = glob.glob(str(Path(self.model_weights_dir, "*.index")))
        if saved_models:
            last_model_path = str(sorted(saved_models)[-1])[:-len(".index")]
            self._log.info(f"Load model from {last_model_path}")
            model.load_weights(last_model_path)
        else:
            self._log.info(f"No saved models in {self.model_weights_dir}")

    def save_model(self):
        # Save the model
        model = self.model

        model_path = str(Path(self.model_weights_dir, datetime.utcnow().isoformat()))
        self._log.debug(f"Save model to {model_path}")
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
            self._log.debug(f"Purged {len(purge_files)} files in {self.model_weights_dir}")

    def save_lastXy(self, X_last: pd.DataFrame, y_pred_last: pd.DataFrame, data_last: Dict[str, pd.DataFrame]):
        """
        Write X,y, data to csv for analysis
        """
        self.X_buf = pd.concat([self.X_buf, X_last])
        self.y_buf = pd.concat([self.y_buf, y_pred_last])
        for data_tag in data_last:
            self.data_bufs[data_tag] = pd.concat([self.data_bufs[data_tag], data_last])

        if datetime.utcnow() - self.last_save_time < self.save_interval:
            return

        self.last_save_time = datetime.utcnow()

        time = X_last.index[-1] if X_last is not None else data_last.index[-1]
        file_name_prefix = f"{pd.to_datetime(time).date()}_{self.ticker}_"

        # Save
        if not self.X_buf.empty:
            Xpath = str(Path(self.model_Xy_dir, file_name_prefix + "X.csv"))
            self._log.debug(f"Saving last X to {Xpath}")
            self.X_buf.to_csv(Xpath, header=not Path(Xpath).exists(), mode='a')
            self.X_buf = pd.DataFrame()
        if not self.y_buf.empty:
            ypath = str(Path(self.model_Xy_dir, file_name_prefix + "y.csv"))
            self._log.debug(f"Saving last y to {ypath}")
            self.y_buf.to_csv(ypath, header=not Path(ypath).exists(), mode='a')
            self.y_buf = pd.DataFrame()
        for data_tag in self.data_bufs:
            if self.data_bufs[data_tag].empty:
                continue
            datapath = str(Path(self.model_Xy_dir, f"{file_name_prefix}{data_tag}.csv"))
            self._log.debug(f"Saving last data to {datapath}")
            self.data_bufs[data_tag].to_csv(datapath, header=not Path(datapath).exists(), mode='a')
            self.data_bufs[data_tag] = pd.DataFrame()
