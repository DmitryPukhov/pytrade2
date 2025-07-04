import logging
import os
from typing import Dict

import boto3
import pandas as pd
from botocore.client import BaseClient


class HistoryS3Downloader:
    """
    Download necessary history data from s3
    """

    def __init__(self, config: Dict, data_dir: str):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info(f"Initializing {self.__class__.__name__}")
        # Try feed specific s3 config then global s3 config
        for prefix in ("pytrade2.feed.s3", "pytrade2.s3"):
            if f"{prefix}.endpoint_url" in config:
                self.s3_access_key = config[f"{prefix}.access_key"]
                self.s3_secret_key = config[f"{prefix}.secret_key"]
                self.s3_bucket = config[f"{prefix}.bucket"]
                self.s3_endpoint_url = config[f"{prefix}.endpoint_url"]
                self._logger.info(f"Data feed s3 is {self.s3_endpoint_url}")
                break

        # local data directory
        self.data_dir = config.get("pytrade2.data.dir") if not data_dir else data_dir

    def read_local_history(self, ticker: str, kind: str, start_date=pd.Timestamp.min,
                           end_date=pd.Timestamp.max) -> pd.DataFrame:
        """ Read data between start_date and end_date from local data directory."""
        accumulated_df = pd.DataFrame()
        local_dir = os.path.join(self.data_dir, "raw", kind)
        for file in os.listdir(local_dir):
            # Check date, encoded in filename like 2025-05-21_BTC-USDT_level2.csv.zip
            datestr = file.split('_')[0]
            file_date = pd.to_datetime(datestr).date()
            if not (start_date <= file_date <= end_date):
                continue
            if not file.endswith(".csv") and not file.endswith(".csv.zip"):
                self._logger.warning(f"Skipping file {file} in {local_dir}")
                continue
            self._logger.debug(f"Reading csv file {file} from {local_dir}")
            df = pd.read_csv(f"{local_dir}/{file}")
            accumulated_df = pd.concat([accumulated_df, df], ignore_index=True)

        # Set index, try to use datetime column for price or candle data
        datetime_col = "datetime" if "datetime" in accumulated_df.columns else "close_time"
        accumulated_df[datetime_col] = pd.to_datetime(accumulated_df[datetime_col])
        accumulated_df.set_index(datetime_col, inplace=True, drop=False)
        return accumulated_df.sort_index()

    def update_local_history(self, ticker: str, start_date=pd.Timestamp.min, end_date=pd.Timestamp.max,
                             kinds=("level2", "candles", "bid_ask")) -> bool:
        """ Download new history data from s3 to local data directory.
        :returns: True if any files were downloaded, False otherwise.
        """
        session = boto3.session.Session()
        s3client = session.client(service_name='s3', endpoint_url=self.s3_endpoint_url,
                                  aws_access_key_id=self.s3_access_key,
                                  aws_secret_access_key=self.s3_secret_key)
        is_new_files = False
        for kind in kinds:
            is_new_files |= self.download_s3_files_between_dates(s3client, self.s3_bucket, ticker,
                                                                 os.path.join("data", "raw", kind),
                                                                 os.path.join(self.data_dir, "raw", kind),
                                                                 start_date,
                                                                 end_date)
        return is_new_files


    def get_download_list(self, s3client: BaseClient, bucket_name, ticker, s3_dir, local_dir, start_date, end_date):
        """ Get list of files to download from s3 directory. Files are between start_date and end_date."""

        # List objects in the bucket
        objects = s3client.list_objects_v2(Bucket=bucket_name, Prefix=s3_dir)

        if 'Contents' not in objects:
            self._logger.info(f"No files found in {bucket_name}/{s3_dir}")

        download_list = []
        # Filter files by date range
        for obj in objects['Contents']:
            s3_file_path = obj['Key']
            file_name = s3_file_path.split('/')[-1]
            local_path = os.path.join(local_dir, file_name)
            # Extract date from filename (assuming format: YYYY-MM-DD_BTC-USDT_level2.csv.zip)
            try:
                file_datetime_str = file_name.split('_')[0]
                file_datetime = pd.to_datetime(file_datetime_str)
                if start_date <= file_datetime.date() <= end_date:
                    # Skip non-csv.zip files
                    if not file_name.endswith('.csv.zip'):
                        self._logger.info(f"Skipping file {s3_file_path}, not a csv.zip file")
                        continue
                    # Append to download list or not
                    local_exists_flag = os.path.exists(local_path)
                    s3_is_newer_flag = local_exists_flag and obj['Size'] != os.path.getsize(local_path)
                    if not local_exists_flag:
                        self._logger.info(f"Local file {local_path} doesn't exist, will download it")
                    elif s3_is_newer_flag:
                        self._logger.info(
                            f"Local file {local_path} size is different from S3 file {s3_file_path}, will download it")
                    # Local file doesn't exist or is older than S3 file
                    if (not local_exists_flag) or s3_is_newer_flag:
                        download_list.append(s3_file_path)
            except (IndexError, ValueError):
                self._logger.info(f"Error parsing date from file {s3_file_path}, skipping")
                continue
        return download_list


    def download_s3_files_between_dates(self, s3client: BaseClient, bucket_name, ticker, s3_dir, local_dir, start_date,
                                        end_date):
        """
        Download files from  S3 directory between specified dates.
        return True if downloaded anything, False if no data downloaded
        """
        self._logger.info(f"Downloading history data from s3://{bucket_name}/{s3_dir} to {local_dir}")

        # Get s3 list of files inside given date range
        download_list = sorted(
            self.get_download_list(s3client, bucket_name, ticker, s3_dir, local_dir, start_date, end_date))
        if not download_list:
            self._logger.info(f"No changed files found in {bucket_name}/{s3_dir} between {start_date} and {end_date}")
            return False
        self._logger.info(
            f"Found {len(download_list)} s3 files to download, from {download_list[0]} to {download_list[-1]}")

        # Create local directory if it doesn't exist
        os.makedirs(local_dir, exist_ok=True)
        for s3_file_path in download_list:
            file_name = s3_file_path.split('/')[-1]
            local_path = os.path.join(local_dir, file_name)
            self._logger.info(f"Downloading {s3_file_path} to {local_path}")
            s3client.download_file(bucket_name, s3_file_path, local_path)
        return True

