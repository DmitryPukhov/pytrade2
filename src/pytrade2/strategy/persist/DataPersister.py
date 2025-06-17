import concurrent.futures.thread
import logging
import os
import threading
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import boto3
import pandas as pd

from pytrade2.strategy.persist.Boto3Hack import Boto3Hack


class DataPersister:
    """ Save the data to local fs, copy to s3"""

    def __init__(self, config: Dict, tag: str):
        self._logger = logging.getLogger(self.__class__.__name__)
        # Hack to fix boto3 issue https://bugs.python.org/issue42647
        threading._register_atexit = Boto3Hack._register_atexit
        concurrent.futures.thread.ThreadPoolExecutor.submit = Boto3Hack.submit

        # Init boto3
        self.s3_enabled = bool(config.get('pytrade2.s3.enabled', 'False').lower() == "true")
        if self.s3_enabled:
            self.s3_access_key = config['pytrade2.s3.access_key']
            self.s3_secret_key = config['pytrade2.s3.secret_key']
            self.s3_bucket = config['pytrade2.s3.bucket']
            self.s3_endpoint_url = config['pytrade2.s3.endpoint_url']

        # Directory for model weights and price data
        self.account_dir = self.db_path = None
        self.data_dir = config.get("pytrade2.data.dir")
        if self.data_dir:
            # Xy data dir
            self.model_xy_dir = str(Path(self.data_dir, tag, "Xy"))
            Path(self.model_xy_dir).mkdir(parents=True, exist_ok=True)
            # Account dir
            self.account_dir = str(Path(self.data_dir, tag, "account"))
            Path(self.account_dir).mkdir(parents=True, exist_ok=True)
            # Database file
            self.db_path = str(Path(self.data_dir, tag, f"{tag}.db"))

        self.last_save_time = datetime.utcnow()
        # Periodically save data
        self.save_interval: timedelta = timedelta(seconds=int(config.get("pytrade2.data.save.interval.sec", 60)))
        self.data_bufs: Dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)
        self.last_learn_saved_index = datetime.min

    def purge_data_files(self, data_dir):
        """ Keep only last day """
        if not data_dir:
            return
        files = os.listdir(data_dir)
        if files:
            keep_prefix = max(files)[:10]  # last yyyy-mm-dd prefix
            self._logger.debug(f"Purging files in {data_dir}, keep only {keep_prefix}")
            for f in files:
                if not f.startswith(keep_prefix):
                    f = os.path.join(data_dir, f)
                    self._logger.debug(f"Purging {f}")
                    os.remove(f)

    def add_to_buf(self, ticker: str,  # X_last: pd.DataFrame, y_pred_last: pd.DataFrame,
                       data_last: Dict[str, pd.DataFrame]):
        """ Add new data to the buffer, don't save now"""
        for data_tag in data_last:
            if data_last[data_tag].empty:
                continue
            if not self.data_bufs[data_tag].empty:
                self.data_bufs[data_tag] = pd.concat([self.data_bufs[data_tag], data_last[data_tag]])
            else:
                self.data_bufs[data_tag] = data_last[data_tag]

    def save_last_data(self, ticker: str,  # X_last: pd.DataFrame, y_pred_last: pd.DataFrame,
                       data_last: Dict[str, pd.DataFrame], mode="a"):
        """
        Write X,y, data to csv for analysis
        """
        # Add new data to the buffer
        self.add_to_buf(ticker, data_last)

        if datetime.utcnow() - self.last_save_time < self.save_interval:
            return
        self.last_save_time = datetime.utcnow()

        for data_tag in self.data_bufs:
            if self.data_bufs[data_tag].empty:
                continue
            # Save dataframe to local file
            file_path = self.persist_df(self.data_bufs[data_tag], self.model_xy_dir, data_tag, ticker, mode = mode)
            self.data_bufs[data_tag] = pd.DataFrame()
            # Data file copy
            self.copy2s3(file_path)

        # Database file copy
        if self.db_path:
            self.copy2s3(Path(self.db_path), compress=False)

        # Account balance copy
        if self.account_dir:
            account_path = Path(self.account_dir, f"{datetime.utcnow().date()}_balance.csv")
            self.copy2s3(account_path)

        # Purge old data
        self.purge_data_files(self.model_xy_dir)
        self.purge_data_files(self.account_dir)

    def persist_df(self, df: pd.DataFrame, dir_: str, data_tag: str, ticker: str, mode = "a") -> Optional[Path]:
        """ Save file locally, append if exists"""
        if df.empty:
            return None
        time = df.index[-1]
        file_name = f"{pd.to_datetime(time).date()}_{ticker}_{data_tag}"
        Path(dir_).mkdir(parents=True, exist_ok=True)  # ensure directory exists
        file_path = Path(dir_, f"{file_name}.csv")
        self._logger.debug(f"Saving last {data_tag} data to {file_path}")
        df.to_csv(str(file_path),
                  header=not file_path.exists(),
                  mode=mode)
        return file_path

    def copy2s3(self, datapath: Path, compress=True):
        if not self.s3_enabled:
            return
        if not os.path.exists(datapath):
            self._logger.debug(f"{datapath} does not exist, cannot upload it to s3")
            return

        if compress:
            # Compress to temp zip before uploading to s3
            zippath = datapath.with_suffix(datapath.suffix + '.zip')
            with zipfile.ZipFile(zippath, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                # csv file inside zip file
                zf.write(datapath, arcname=datapath.name)
            datapath = zippath

        # Upload
        s3datapath = str(datapath).lstrip("../")
        self._logger.debug(f"Uploading {datapath} to s3://{self.s3_bucket}/{s3datapath}")

        s3 = boto3.client(service_name='s3', endpoint_url=self.s3_endpoint_url, aws_access_key_id=self.s3_access_key,
                          aws_secret_access_key=self.s3_secret_key)
        s3.upload_file(datapath, self.s3_bucket, s3datapath)
        self._logger.debug(f"Uploaded s3://{self.s3_bucket}/{s3datapath}")

        # Delete temp zip
        if compress:
            os.remove(datapath)
