import asyncio
import datetime
import pandas as pd


class HistoryCache:
    """In-memory history quote cache for speed up Quik historical data fetching"""
    def __init__(self, class_code, sec_code, interval, cache_update_min_interval_sec=0.2):
        self.class_code = class_code
        self.sec_code = sec_code
        self.interval = interval
        self._data = None
        self._last_quote = datetime.datetime(1900, 1, 1)
        self.ds_uuid = None
        self._lock = asyncio.Lock()
        self._last_update = None
        self._cache_update_interval_sec = cache_update_min_interval_sec

    def process_history(self, quotes_df):
        assert isinstance(quotes_df, pd.DataFrame), 'quotes_df must be a Pandas DataFrame'
        assert isinstance(quotes_df.index, pd.DatetimeIndex), 'quotes_df must have DatetimeIndex'
        assert quotes_df.index.is_monotonic_increasing, 'quotes_df must be sorted by date ascending'

        if self._data is None:
            if len(quotes_df) > 0:
                self._data = quotes_df
                self._last_quote = quotes_df.index[-1]
                self._last_update = datetime.datetime.now()
        else:
            if len(quotes_df) > 0:
                # Update data and rewrite overlapping records in old data
                self._data = quotes_df.combine_first(self._data)
                self._last_quote = quotes_df.index[-1]
                self._last_update = datetime.datetime.now()

    @property
    def data(self):
        return self._data

    @property
    def last_bar_date(self):
        return self._last_quote

    @property
    def lock(self):
        return self._lock

    @property
    def can_update(self):
        if self._last_update is None:
            # Cache is new, let it update
            return True
        else:
            # Permit cache update only if self._cache_update_interval_sec passed
            return (datetime.datetime.now() - self._last_update).total_seconds() > self._cache_update_interval_sec

