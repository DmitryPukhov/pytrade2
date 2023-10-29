import glob
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict
import boto3
import pandas as pd
from keras.models import Model


class PersistableStateStrategy:
    """ Strategy whicn can save the data and read/write model weights."""

    def __init__(self, config: Dict):
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
        self.X_buf = pd.DataFrame()
        self.y_buf = pd.DataFrame()
        self.data_bufs: Dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)
        self.last_learn_saved_index = datetime.min

    def load_last_model(self, model: Model):
        saved_models = glob.glob(str(Path(self.model_weights_dir, "*.index")))
        if saved_models:
            last_model_path = str(sorted(saved_models)[-1])[:-len(".index")]
            logging.info(f"Load model from {last_model_path}")
            model.load_weights(last_model_path)
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
            keep_prefix = max(files)[:10]
            #keep_prefix = [f[:10] for f in files][-1]  # last yyyy-mm-dd prefix
            logging.debug(f"Purging files in {data_dir}, keep only {keep_prefix}")
            for f in files:
                if not f.startswith(keep_prefix):
                    f = os.path.join(data_dir, f)
                    logging.debug(f"Purging {f}")
                    os.remove(f)

    def save_learn_xy_new(self, ticker: str, x: pd.DataFrame, y: pd.DataFrame):
        """ From given x, y save rows after stored time index """

        x_path = self.file_path_of(ticker, x.index[-1], "learn_x")
        x[x.index > self.last_learn_saved_index].to_csv(x_path, header=not Path(x_path).exists(), mode='a')

        y_path = self.file_path_of(ticker, x.index[-1], "learn_y")
        y[x.index > self.last_learn_saved_index].to_csv(y_path, header=not Path(y_path).exists(), mode='a')

        self.copy2s3(x_path)
        self.copy2s3(y_path)

        self.last_learn_saved_index = x.index[-1]

    def file_path_of(self, ticker: str, time: pd.Timestamp, tag: str):
        file_name_prefix = f"{pd.to_datetime(time).date()}_{ticker}"
        file_path = str(Path(self.model_Xy_dir, f"{file_name_prefix}_{tag}.csv"))
        return file_path

    def save_last_data(self, ticker: str,  # X_last: pd.DataFrame, y_pred_last: pd.DataFrame,
                       data_last: Dict[str, pd.DataFrame]):
        """
        Write X,y, data to csv for analysis
        """
        # self.X_buf = pd.concat([self.X_buf, X_last])
        # self.y_buf = pd.concat([self.y_buf, y_pred_last])
        for data_tag in data_last:
            self.data_bufs[data_tag] = pd.concat([self.data_bufs[data_tag], data_last[data_tag]])

        if datetime.utcnow() - self.last_save_time < self.save_interval:
            return

        self.last_save_time = datetime.utcnow()

        for data_tag in self.data_bufs:
            if self.data_bufs[data_tag].empty:
                continue
            time = self.data_bufs[data_tag].index[-1]
            datapath = self.file_path_of(ticker, time, data_tag)
            logging.debug(f"Saving last {data_tag} data to {datapath}")
            self.data_bufs[data_tag].to_csv(datapath, header=not Path(datapath).exists(), mode='a')
            self.data_bufs[data_tag] = pd.DataFrame()
            # Data file copy
            self.copy2s3(datapath)

        # Database file copy
        self.copy2s3(self.db_path)

        # Account balance copy
        account_path = str(Path(self.account_dir, f"{datetime.utcnow().date()}_balance.csv"))
        self.copy2s3(account_path)

        # Purge old data
        self.purge_data_files(self.model_Xy_dir)
        self.purge_data_files(self.account_dir)

    def copy2s3(self, datapath: str):
        if not self.s3_enabled:
            return
        if not os.path.exists(datapath):
            logging.info(f"{datapath} does not exist, cannot upload it to s3")
            return

        s3datapath = datapath.lstrip("../")
        logging.debug(f"Uploading {datapath} to s3://{self.s3_bucket}/{s3datapath}")
        session = boto3.Session(aws_access_key_id=self.s3_access_key, aws_secret_access_key=self.s3_secret_key)
        s3 = session.client(service_name='s3', endpoint_url=self.s3_endpoint_url)
        s3.upload_file(datapath, self.s3_bucket, s3datapath)
        logging.debug(f"Uploaded s3://{self.s3_bucket}/{s3datapath}")
