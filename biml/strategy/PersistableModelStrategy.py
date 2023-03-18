import glob
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

from keras.models import Model


class PersistableModelStrategy:
    def __init__(self, config: Dict):
        self._log = logging.getLogger(self.__class__.__name__)
        models_dir = config["biml.model.dir"]
        self.model_weights_dir = str(Path(models_dir, self.__class__.__name__, "weights"))

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
