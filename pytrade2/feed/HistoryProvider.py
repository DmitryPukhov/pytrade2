import logging
import logging
import os
from typing import Dict

import boto3
import pandas as pd
from botocore.client import BaseClient


class HistoryProvider:
    """
    Download necessary history data from s3
    """

    def __init__(self, config: Dict):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info("Initializing HistoryProvider")
        self.s3_access_key = config['pytrade2.s3.access_key']
        self.s3_secret_key = config['pytrade2.s3.secret_key']
        self.s3_bucket = config['pytrade2.s3.bucket']
        self.s3_endpoint_url = config['pytrade2.s3.endpoint_url']
        self.data_dir = config.get("pytrade2.data.dir")

    def read_local_history(self, kind: str, start_date = pd.Timestamp.min, end_date = pd.Timestamp.max) -> pd.DataFrame:
        """ Read data between start_date and end_date from local data directory."""
        accumulated_df = pd.DataFrame()
        local_dir = f"{self.data_dir}/raw/{kind}/"
        for file in os.listdir(local_dir):
            # Check date, encoded in filename like 2025-05-21_BTC-USDT_level2.csv.zip
            datestr = file.split('_')[0]
            file_date = pd.to_datetime(datestr)
            if not (start_date <= file_date <= end_date):
                continue
            df = pd.read_csv(f"{local_dir}/{file}")
            accumulated_df = pd.concat([accumulated_df, df], ignore_index=True)

        # Set index, try to use datetime column for price or candle data
        index_col = "datetime" if "datetime" in accumulated_df.columns else "close_time"
        accumulated_df.set_index(index_col, inplace=True, drop=False)
        return accumulated_df.sort_index()


    def update_local_history(self, start_date = pd.Timestamp.min, end_date = pd.Timestamp.max,
                    kinds=("level2", "candles", "bid_ask")):
        """ Download new history data from s3 to local data directory."""
        try:
            session = boto3.session.Session()
            s3client = session.client(service_name='s3', endpoint_url=self.s3_endpoint_url,
                                      aws_access_key_id=self.s3_access_key,
                                      aws_secret_access_key=self.s3_secret_key)
            for kind in kinds:
                self.download_s3_files_between_dates(s3client, self.s3_bucket, f"data/raw/{kind}/",
                                                     f"{self.data_dir}/raw/{kind}/", start_date, end_date)
        except Exception as e:
            self._logger.error(f"Error downloading history data: {e}")

    def get_download_list(self, s3client: BaseClient, bucket_name, s3_dir, local_dir, start_date, end_date):
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
                date_str = file_name.split('_')[0]
                file_date = pd.to_datetime(date_str)
                if start_date <= file_date <= end_date:
                    local_exists_flag = os.path.exists(local_path)
                    s3_is_newer_flag = local_exists_flag and obj['Size'] != os.path.getsize(local_path)
                    if not local_exists_flag:
                        self._logger.info(f"Local file {local_path} doesn't exist, will download it")
                    elif s3_is_newer_flag:
                        self._logger.info(f"Local file {local_path} size is different from S3 file {s3_file_path}, will download it")
                    # Local file doesn't exist or is older than S3 file
                    if (not local_exists_flag) or s3_is_newer_flag:
                        download_list.append(s3_file_path)
            except (IndexError, ValueError):
                self._logger.info(f"Error parsing date from file {s3_file_path}, skipping")
                continue
        return download_list

    def download_s3_files_between_dates(self, s3client: BaseClient, bucket_name, s3_dir, local_dir, start_date,
                                        end_date):
        """
        Download files from  S3 directory between specified dates.
        """
        self._logger.info(f"Downloading history data from s3://{bucket_name}/{s3_dir} to {local_dir}")

        # Get s3 list of files inside given date range
        download_list = sorted(self.get_download_list(s3client, bucket_name, s3_dir, local_dir, start_date, end_date))
        if not download_list:
            self._logger.info(f"No files found in {bucket_name}/{s3_dir} between {start_date} and {end_date}")
            return
        self._logger.info(f"Found {len(download_list)} s3 files to download, from {download_list[0]} to {download_list[-1]}")

        # Create local directory if it doesn't exist
        os.makedirs(local_dir, exist_ok=True)
        for s3_file_path in download_list:
            file_name = s3_file_path.split('/')[-1]
            local_path = os.path.join(local_dir, file_name)
            self._logger.info(f"Downloading {s3_file_path} to {local_path}")
            s3client.download_file(bucket_name, s3_file_path, local_path)

