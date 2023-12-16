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


class PersistableStateStrategy:
    """ Strategy whicn can save the data and read/write model weights."""
    @staticmethod
    def _register_atexit(func, *arg, **kwargs):
        """
        Hack to fix boto3 threading issue: https://bugs.python.org/issue42647
        """
        # This code is commented to fix boto3 error
        # if threading._SHUTTING_DOWN:
        #     raise RuntimeError("can't register atexit after shutdown")

        call = functools.partial(func, *arg, **kwargs)
        threading._threading_atexits.append(call)

    def __init__(self, config: Dict):
        # Hack to fix boto3 issue https://bugs.python.org/issue42647
        threading._register_atexit = PersistableStateStrategy._register_atexit

        # Init boto3
        self.s3_enabled = config.get('pytrade2.s3.enabled', False)
        if self.s3_enabled:
            self.s3_access_key = config['pytrade2.s3.access_key']
            self.s3_secret_key = config['pytrade2.s3.secret_key']
            self.s3_bucket = config['pytrade2.s3.bucket']
            self.s3_endpoint_url = config['pytrade2.s3.endpoint_url']

        # Directory for model weights and price data
        self.data_dir = config["pytrade2.data.dir"]
        if self.data_dir:
            # weights dir
            self.model_weights_dir = str(Path(self.data_dir, self.__class__.__name__, "weights"))
            Path(self.model_weights_dir).mkdir(parents=True, exist_ok=True)
            # Xy data dir
            self.model_Xy_dir = str(Path(self.data_dir, self.__class__.__name__, "Xy"))
            Path(self.model_Xy_dir).mkdir(parents=True, exist_ok=True)
            # Account dir
            self.account_dir = str(Path(self.data_dir, self.__class__.__name__, "account"))
            Path(self.account_dir).mkdir(parents=True, exist_ok=True)
            # Database file
            self.db_path = str(Path(self.data_dir, self.__class__.__name__, f"{self.__class__.__name__}.db"))

        self.last_save_time = datetime.utcnow()
        self.model = None
        # Save data each 10 seconds
        self.save_interval: timedelta = timedelta(seconds=60)
        self.data_bufs: Dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)
        self.last_learn_saved_index = datetime.min

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

    def save_model(self):
        # Save the model
        model = self.model

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

    @staticmethod
    def purge_data_files(data_dir):
        """ Keep only last day """

        files = os.listdir(data_dir)
        if files:
            keep_prefix = max(files)[:10]  # last yyyy-mm-dd prefix
            logging.debug(f"Purging files in {data_dir}, keep only {keep_prefix}")
            for f in files:
                if not f.startswith(keep_prefix):
                    f = os.path.join(data_dir, f)
                    logging.debug(f"Purging {f}")
                    os.remove(f)

    def save_last_data(self, ticker: str,  # X_last: pd.DataFrame, y_pred_last: pd.DataFrame,
                       data_last: Dict[str, pd.DataFrame]):
        """
        Write X,y, data to csv for analysis
        """
        for data_tag in data_last:
            self.data_bufs[data_tag] = pd.concat(
                [df for df in [self.data_bufs[data_tag], data_last[data_tag]] if not df.empty])

        if datetime.utcnow() - self.last_save_time < self.save_interval:
            return

        self.last_save_time = datetime.utcnow()

        for data_tag in self.data_bufs:
            if self.data_bufs[data_tag].empty:
                continue
            time = self.data_bufs[data_tag].index[-1]
            file_name = f"{pd.to_datetime(time).date()}_{ticker}_{data_tag}"
            file_path = Path(self.model_Xy_dir, f"{file_name}.csv")
            logging.debug(f"Saving last {data_tag} data to {file_path}")
            self.data_bufs[data_tag].to_csv(str(file_path),
                                            header=not file_path.exists(),
                                            mode='a')
            self.data_bufs[data_tag] = pd.DataFrame()
            # Data file copy
            self.copy2s3(file_path)

        # Database file copy
        self.copy2s3(Path(self.db_path), compress=False)

        # Account balance copy
        account_path = Path(self.account_dir, f"{datetime.utcnow().date()}_balance.csv")
        self.copy2s3(account_path)

        # Purge old data
        self.purge_data_files(self.model_Xy_dir)
        self.purge_data_files(self.account_dir)

    def copy2s3(self, datapath: Path, compress=True):
        if not self.s3_enabled:
            return
        if not os.path.exists(datapath):
            logging.info(f"{datapath} does not exist, cannot upload it to s3")
            return

        if compress:
            # Compress to temp zip before uploading to s3
            zippath = datapath.with_suffix(datapath.suffix+'.zip')
            with zipfile.ZipFile(zippath, 'w') as zf:
                # csv file inside zip file
                zf.write(datapath, arcname=datapath.name)
            datapath=zippath

        # Upload
        s3datapath = str(datapath).lstrip("../")
        logging.debug(f"Uploading {datapath} to s3://{self.s3_bucket}/{s3datapath}")

        session = boto3.Session(aws_access_key_id=self.s3_access_key, aws_secret_access_key=self.s3_secret_key)
        s3 = session.resource(service_name='s3', endpoint_url=self.s3_endpoint_url) # error is here
        s3.meta.client.upload_file(datapath, self.s3_bucket, s3datapath)
        logging.debug(f"Uploaded s3://{self.s3_bucket}/{s3datapath}")

        # Delete temp zip
        if compress:
            os.remove(datapath)



