import glob
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import pandas as pd
from keras.models import Model


class PersistableModelStrategy:
    def __init__(self, config: Dict):
        self._log = logging.getLogger(self.__class__.__name__)

        # Directory for model weights and price data
        self.model_dir = config["biml.model.dir"]
        if self.model_dir:
            self.model_weights_dir = str(Path(self.model_dir, self.__class__.__name__, "weights"))
            self.model_Xy_dir = str(Path(self.model_dir, self.__class__.__name__, "Xy"))
            Path(self.model_Xy_dir).mkdir(parents=True, exist_ok=True)
        self.last_save_time = datetime.utcnow()
        # Save data each 10 seconds
        self.save_interval: timedelta = timedelta(seconds=10)
        self.X_buf = pd.DataFrame()
        self.y_buf = pd.DataFrame()
        self.data_buf = pd.DataFrame()

    def load_last_model(self, model: Model):
        saved_models = glob.glob(str(Path(self.model_weights_dir, "*.index")))
        if saved_models:
            last_model_path = str(sorted(saved_models)[-1])[:-len(".index")]
            self._log.debug(f"Load model from {last_model_path}")
            model.load_weights(last_model_path)
        else:
            self._log.info(f"No saved models in {self.model_weights_dir}")

    def save_model(self):
        # Save the model
        model: Model = self.model.regressor.named_steps["model"].model

        model_path = str(Path(self.model_weights_dir, datetime.utcnow().isoformat()))
        self._log.debug(f"Save model to {model_path}")
        model.save_weights(model_path)

    def save_lastXy(self, X_last: pd.DataFrame, y_pred_last: pd.DataFrame, data_last: pd.DataFrame):
        """
        Write X,y, data to csv for analysis
        """
        self.X_buf.append(X_last)
        self.y_buf.append(y_pred_last)
        self.data_buf.append(data_last)

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
            self._log.debug(f"Saving  last y to {ypath}")
            # y_pred_last_df = pd.DataFrame(index=X_last.index, data=y_pred_last,
            #                               columns=["fut_delta_low", "fut_candle_size"])
            #y_pred_last_df.to_csv(ypath, header=not Path(ypath).exists(), mode='a')
            self.y_buf.to_csv(ypath, header=not Path(ypath).exists(), mode='a')
            self.y_buf = pd.DataFrame()
        if not self.data_buf.empty:
            datapath = str(Path(self.model_Xy_dir, file_name_prefix + "data.csv"))
            self._log.debug(f"Saving last data to {datapath}")
            self.data_buf.to_csv(datapath, header=not Path(datapath).exists(), mode='a')
            self.data_buf = pd.DataFrame()
