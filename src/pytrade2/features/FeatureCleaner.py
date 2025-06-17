import pandas as pd


class FeatureCleaner:
    """ Exclude input time gaps from pytrade2.features"""

    @classmethod
    def find_time_gaps(cls, dfindex: pd.DatetimeIndex, start_delta: pd.Timedelta = pd.Timedelta(0), freq='1min') -> [()]:
        """
        Find intervals where data is missing in a time series DataFrame.
        :returns: list of tuples with (gap_start, gap_end) timestamps
        """
        if dfindex.empty:
            return []

        # Generate expected timestamps at the specified frequency
        full_range = pd.date_range(start=dfindex.min(),
                                   end=dfindex.max(),
                                   freq=freq)

        # Find missing timestamps
        missing_times = full_range.difference(dfindex)

        # Group consecutive missing timestamps into intervals
        gaps = [(dfindex.min(), dfindex.min() + start_delta)]
        if len(missing_times) > 0:
            current_start = missing_times[0]
            prev_time = missing_times[0]

            for time in missing_times[1:]:
                if time != prev_time + pd.Timedelta(freq):
                    # Gap ends here
                    gaps.append((current_start, prev_time))
                    current_start = time
                prev_time = time

            # Add the last gap
            gaps.append((current_start, prev_time))

        return [(start, end + start_delta) for start, end in gaps]

    @classmethod
    def get_gap_mask(cls, df: pd.DataFrame, gaps: [()]):
        """ Mask time series: True if df record good, bad if in time gap """

        intervals = pd.IntervalIndex.from_tuples(
            [(pd.to_datetime(start), pd.to_datetime(end)) for start, end in gaps],
            closed='both'
        )

        # Vectorized check for timestamps in gaps
        def is_in_any_gap(ts):
            return any(interval.left <= ts <= interval.right for interval in intervals)

        # Create mask (True = gap, False = not in gap)
        return df.index.to_series().apply(is_in_any_gap)

    @classmethod
    def exclude_gaps(cls, df: pd.DataFrame, gaps: [()]):
        """
        More efficient approach when dealing with many gap intervals.
        df : The input DataFrame containing time series data to be filtered.
        gaps : List of gap intervals as (start_time, end_time) tuples.
        time_col : Name of the column containing the timestamps to check against gaps.
        start_delta: we can start aggregation only after some delta after start to fill the aggregation window
        """
        mask = cls.get_gap_mask(df, gaps)
        return df[~mask]

    @classmethod
    def clean(cls, input_df: pd.DataFrame, features_df: pd.DataFrame, start_delta: pd.Timedelta = pd.Timedelta(0)):
        """ Find gaps in input df and exclude bad features inside gaps and before start_delta after each gap """
        gaps = cls.find_time_gaps(input_df.index, start_delta)
        return cls.exclude_gaps(features_df, gaps)
