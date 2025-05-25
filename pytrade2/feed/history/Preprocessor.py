import logging
import os
import pathlib
import pandas as pd

from features.level2.Level2Features import Level2Features


class Preprocessor:
    def __init__(self, data_dir: str = "./data"):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.data_dir = data_dir

        self.data_dir_raw = f"{self.data_dir}/raw"
        self.data_dir_preproc = f"{self.data_dir}/preproc"

    def datetime_col(self, df):
        """ return close_time for candles, datetime for level2 or bidask"""
        return "datetime" if "datetime" in df.columns else "close_time"

    @staticmethod
    def level2_transform(df_level2_raw):
        df_level2 = Level2Features().expectation(df_level2_raw)
        df_level2["l2_bid_ask_vol"] = df_level2["l2_bid_vol"] + df_level2["l2_ask_vol"]
        return df_level2

    def get_unprocessed_raw_files(self, kind):
        """
        Compare raw files dir with preprocessed dir, get not processed raw files
        Raw files have .csv.zip extension, processed are with .csv
        """

        raw_dir_kind = os.path.join(self.data_dir_raw, kind)
        os.makedirs(raw_dir_kind, exist_ok=True)

        preproc_dir_kind = os.path.join(self.data_dir_preproc, kind)
        os.makedirs(preproc_dir_kind, exist_ok=True)

        # Fill unprocessed file list
        unprocessed_list = []
        for raw_file in sorted(os.listdir(raw_dir_kind)):
            if not raw_file.endswith(".csv.zip") and not raw_file.endswith(".csv"):
                self._logger.warning(f"Raw file {raw_file} is not *.csv or *.csv.zip, skipping")
                continue
            # If raw file is *.csv.zip, preproc will be *.csv not zipped
            preproc_file = pathlib.Path(raw_file).stem if raw_file.endswith(".zip") else raw_file
            raw_file_path = os.path.join(raw_dir_kind, raw_file)
            preproc_file_path = os.path.join(preproc_dir_kind, f"{preproc_file}")

            preproc_not_exist = not os.path.exists(preproc_file_path)
            old_preproc_flag = False if preproc_not_exist else os.path.getmtime(raw_file_path) > os.path.getmtime(
                preproc_file_path)
            if preproc_not_exist:
                self._logger.info(f"Preprocessed {preproc_file} does not exist, will preprocess raw {raw_file}")
            elif old_preproc_flag:
                self._logger.info(f"Preprocessed {preproc_file} is older than raw, will preprocess raw {raw_file}")

            if preproc_not_exist or old_preproc_flag:
                # Already preprocessed
                unprocessed_list.append(raw_file)
        return unprocessed_list

    def transform(self, df: pd.DataFrame, kind: str):
        """ Transform different kinds of data"""

        # Clean not needed columns
        datetime_col = self.datetime_col(df)
        # drop_cols = ["datetime", f"{datetime_col}.1", "ticker", "symbol"]
        drop_cols = [f"{datetime_col}.1", "ticker", "symbol"]
        for drop_col in drop_cols:
            if drop_col in df.columns:
                del df[drop_col]

        # Transform
        if kind == "level2":
            df = self.level2_transform(df)
        elif kind == "candles":
            df = df.resample("1min", label="right", closed="right").agg(
                {"open_time": "last", "close_time": "last", "open": "first", "high": "max", "low": "min",
                 "close": "last", "vol": "sum"})
        else:
            df = df.resample("1min", label="right", closed="right").agg("mean")
        return df

    def preprocess_last_raw_data(self, ticker: str, kind: str):
        """ Read raw data, resample to 1min, write to preprocessed dir. Needed to reduce data amount """

        source_dir = f"{self.data_dir_raw}/{kind}"
        target_dir = f"{self.data_dir_preproc}/{kind}"
        os.makedirs(target_dir, exist_ok=True)

        # Not preprocessed or newer raw files
        unprocessed_raw_files = self.get_unprocessed_raw_files(kind)

        file_paths = [f"{source_dir}/{f}" for f in unprocessed_raw_files]
        self._logger.info(f"Preprocess {len(file_paths)} new {kind} raw files")
        last_preproc_date = pd.Timestamp.min
        for raw_file_path in file_paths:
            self._logger.info(f"Read {ticker} {kind} data from {raw_file_path}")
            # Read raw data
            df = pd.read_csv(raw_file_path, parse_dates=True)
            datetime_col = self.datetime_col(df)
            df[datetime_col] = pd.to_datetime(df[datetime_col])
            df.set_index(datetime_col, drop=False, inplace=True)

            # Raw -> preprocessed transformation
            df = self.transform(df, kind)
            if not df.empty:
                last_preproc_date = max(df.index[-1], last_preproc_date)

            # Prepare target path
            target_file_name = pathlib.Path(raw_file_path).stem
            if not target_file_name.endswith("csv"): target_file_name += ".csv"
            preprocessed_file_path = os.path.join(target_dir, target_file_name)

            # Write to preprocessed dir
            self._logger.info(f"Write preprocessed {ticker} {kind} data to {preprocessed_file_path}")
            df.to_csv(preprocessed_file_path, header=True)
        return last_preproc_date

    def read_last_preproc_data(self, ticker: str, kind: str, days=1):
        """ Read last given days from preprocessed directory """

        source_dir = f"{self.data_dir_preproc}/{kind}"
        self._logger.info(f"Read {ticker} {kind} data for {days} days from {source_dir}")

        file_paths = sorted(
            [f"{source_dir}/{f}" for f in os.listdir(source_dir) if f.endswith(".csv")])[-days:]

        df = pd.concat([pd.read_csv(f, parse_dates=True) for f in file_paths])
        datetime_col = self.datetime_col(df)
        df = self.clean_columns(df)
        return df

    def clean_columns(self, df: pd.DataFrame):
        """ After level2, bidask or candles df has been read, set datetime index, clean columns which are not needed"""
        datetime_col = self.datetime_col(df)
        remove_cols = [f"{datetime_col}.1", "ticker", "symbol"]
        for remove_col in remove_cols:
            if remove_col in df.columns:
                del df[remove_col]
        df[datetime_col] = pd.to_datetime(df[datetime_col])

        if "close_time" in df.columns and "vol" in df.columns:
            # remove duplicated
            df = df.groupby(df["close_time"]) \
                .apply(lambda x: x[x['vol'] == x['vol'].max()]) \
                .reset_index(drop=True)
            # df = df.loc[df.groupby(df['close_time'])['vol'].idxmax()] #.reset_index(drop=True)

        df = df.set_index(datetime_col, drop=False, inplace=False)
        return df
