import glob
import logging
from datetime import datetime
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

    def save_lastXy(self, X_last: pd.DataFrame, y_pred_last, data_last: pd.DataFrame):
        """
        Write X,y, data to csv for analysis
        """
        time = X_last.index[-1] if X_last is not None else data_last.index[-1]
        file_name_prefix = f"{pd.to_datetime(time).date()}_{self.ticker}_"

        # Save
        if X_last is not None:
            Xpath = str(Path(self.model_Xy_dir, file_name_prefix + "X.csv"))
            self._log.debug(f"Saving last X to {Xpath}")
            X_last.to_csv(Xpath, header=not Path(Xpath).exists(), mode='a')
        if y_pred_last is not None:
            ypath = str(Path(self.model_Xy_dir, file_name_prefix + "y.csv"))
            self._log.debug(f"Saving  last y to {ypath}")
            y_pred_last_df = pd.DataFrame(index=X_last.index, data=y_pred_last,
                                          columns=["fut_delta_low", "fut_candle_size"])
            y_pred_last_df.to_csv(ypath, header=not Path(ypath).exists(), mode='a')
        if data_last is not None:
            datapath = str(Path(self.model_Xy_dir, file_name_prefix + "data.csv"))
            self._log.debug(f"Saving last data to {datapath}")
            data_last.to_csv(datapath, header=not Path(datapath).exists(), mode='a')
