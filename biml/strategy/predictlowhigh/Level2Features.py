import pandas as pd
import numpy as np


class Level2Features:
    """
    Level2 feature engineering
    """

    def level2_buckets(self, level2: pd.DataFrame, l2size: int = 0, buckets: int = 20) -> pd.DataFrame:
        """
        Return dataframe with level2 feature columns. Colums are named "bucket<n>"
        where n in a number of price interval and value is summary volumes inside this price.
        For ask price intervals number of buckets >0, for bid ones < 0
        level2: DataFrame with level2 tick columns: datetime, price, bid_vol, ask_vol
        level2 price and volume for each time
        """
        # Assign bucket number for each level2 item
        level2.set_index("datetime")
        level2 = self.assign_bucket(level2, l2size, buckets)

        # Pivot buckets to feature columns: bucket_1, bucket_2 etc. with summary bucket's volume as value.
        maxbucket = buckets // 2 - 1
        minbucket = -buckets // 2
        askfeatures = self.pivot_buckets(level2, 'ask_vol', 0, maxbucket)
        bidfeatures = self.pivot_buckets(level2, 'bid_vol', minbucket, -1)

        # Ask + bid buckets
        level2features = bidfeatures.merge(askfeatures, on='datetime')
        return level2features

    def assign_bucket(self, level2: pd.DataFrame, l2size: int = 0, buckets: int = 20) -> pd.DataFrame:
        """
        To each level2 item set it's bucket number.
        l2size: max-min price across all level2 snapshots
        buckets: split level2 snapshots to this number of items, calculate volume inside each bucket
        """
        # Calc middle price between ask and bid
        level2 = level2.set_index("datetime")
        askmin = level2[level2['ask_vol'].notna()].groupby('datetime')['price'].min().reset_index().set_index(
            "datetime")
        level2['price_min'] = askmin['price']
        bidmax = level2[level2['bid_vol'].notna()].groupby('datetime')['price'].max().reset_index().set_index(
            "datetime")
        level2['price_max'] = bidmax['price']
        level2['price_middle'] = (askmin['price'] + bidmax['price']) / 2

        # Assign a bucket number to each level2 item
        # scalar level2 size and bucket size
        if not l2size:
            l2size = level2.groupby('datetime')['price'].agg(np.ptp).reset_index()['price'].median()
        # 10 ask steps + 10 bid steps
        # buckets = 20
        bucketsize = l2size / buckets

        # If price is too out, set maximum possible bucket
        level2['bucket'] = (level2['price'] - level2['price_middle']) // bucketsize
        maxbucket = buckets // 2 - 1
        minbucket = -buckets // 2
        level2['bucket'] = level2['bucket'].clip(upper=maxbucket, lower=minbucket)
        return level2

    def pivot_buckets(self, level2: pd.DataFrame, vol_col_name: str, minbucket: int, maxbucket: int) -> pd.DataFrame:
        """
        Pivot dataframe to make bucket columns with volume values
        """
        # Calculate volume inside each group
        grouped = level2[level2['bucket'].between(minbucket, maxbucket)].groupby(['datetime', 'bucket'])[
            vol_col_name].sum().reset_index(level=1)

        grouped['bucket'] = grouped['bucket'].astype(int)
        features = grouped.reset_index().pivot_table(index='datetime', columns='bucket', values=vol_col_name)
        # Add absent buckets (rare case)
        for col in range(minbucket, maxbucket + 1):
            if col not in features.columns:
                features[col] = 0
        features = features[sorted(features)]

        features.columns = ['l2_bucket_' + str(col) for col in features.columns]
        return features
