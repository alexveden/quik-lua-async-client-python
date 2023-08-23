import asyncio
import datetime
from math import nan
from typing import Dict, List, Any, Tuple, Optional

import pandas as pd

from .errors import QuikLuaException, QuikLuaConnectionException


class ParamWatcher:
    """
    A special class that decides which params have to be updated based on given interval and time passed since last update.
    """
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self._watched = pd.DataFrame(columns=['dt', 'key'])
        self._watched['dt'] = self._watched['dt'].astype(float)

    def count(self) -> int:
        return len(self._watched)

    def subscribed(self, param_tuple_list: List[Tuple]) -> None:
        dt_now = datetime.datetime.now().timestamp()
        for (class_code, sec_code, param, update_interval) in param_tuple_list:
            param = param.lower()
            idx_key = '__'.join((class_code, sec_code, param))
            self._watched.at[idx_key, 'dt'] = dt_now
            self._watched.at[idx_key, 'key'] = (class_code, sec_code, param)
            self._watched.at[idx_key, 'upd_int'] = update_interval

    def unsubscribed(self, param_tuple_list: List[Tuple]) -> None:
        for (class_code, sec_code, param) in param_tuple_list:
            param = param.lower()
            idx_key = '__'.join((class_code, sec_code, param))
            try:
                del self._watched[idx_key]
            except KeyError:
                # All good, index was deleted
                pass

    def get_update_candidates(self) -> Any:
        if len(self._watched) == 0:
            return []
        last_upd = self._watched['dt'] + self._watched['upd_int']
        dt_now = datetime.datetime.now().timestamp()
        return self._watched[last_upd < dt_now]

    def set_candidate_updates(self, candidates: Any) -> None:
        if len(self._watched) != 0:
            dt_now = datetime.datetime.now().timestamp()
            self._watched.loc[candidates.index, 'dt'] = dt_now


class ParamCache:
    """
    Current parameters (bid/ask/quotes, etc.) cache table

    It can store and parse everything that returned by getParamEx2() RPC

    Values are parsed depending on their types.

    Missing values marked as None (for stings, dates, times) or nan (for numeric).
    """
    def __init__(self, class_code: str, sec_code: str, params_list: List[str]):
        self.class_code = class_code
        self.sec_code = sec_code
        if params_list is None or len(params_list) == 0:
            raise ValueError('params_list is empty')
        self.params: Dict[str, Any] = {p.lower(): None for p in params_list}
        self.last_quote_change_utc: Optional[datetime.datetime] = None

    def process_param(self, param_key: str, param_ex_api_response: dict) -> None:
        """
        Fills the parameter cache based on getParamEx2() RPC response

        :param param_key: param field name, must be `params_list` in constructor
        :param param_ex_api_response: as {'param_ex': {'param_type': '1',  'result': '1',  'param_image': '152 420',  'param_value': '152420.000000'}}
        :return:
        """
        key = param_key.lower()
        assert key in self.params, f'Param key {key} is not requested at ParamCache constructor'
        assert 'param_ex' in param_ex_api_response, \
            f'Expected to get getParamEx2() RPC response format, got ({param_ex_api_response})'

        res = param_ex_api_response['param_ex']

        if res['result'] != '1':
            if key in self.params:
                # Already in params, highly likely this occurs after disconnect
                raise QuikLuaConnectionException(f'({self.class_code},{self.sec_code}):'
                                                 f' missing param valid param `{key}`, possibly after disconnect')
            # Highly likely it's unknown param_type / or key
            raise QuikLuaException(f'({self.class_code},{self.sec_code}):'
                                   f' getParamEx2() unknown or invalid param key: {param_key}: {res}')

        if res['param_type'] == '1' or res['param_type'] == '2':
            # Float or Int (but parse as floats)
            if res['result'] == '1':
                # Request was successful
                _val = float(res['param_value'])
                if self.params.get(key) != _val:
                    self.last_quote_change_utc = datetime.datetime.utcnow()
                self.params[key] = _val
            else:
                self.params[key] = nan
        elif res['param_type'] == '3' or res['param_type'] == '4':
            # String or enumerable
            if res['result'] == '1':
                # Request was successful
                self.params[key] = res['param_image']
            else:
                self.params[key] = None
        elif res['param_type'] == '5':
            # Time
            if res['result'] == '1' and res['param_image']:
                # Request was successful
                t_str = res['param_image']

                if ':' not in t_str:
                    raise QuikLuaException(f'Unknown param time format {key}: {t_str}')
                t_tok = t_str.split(':')
                self.params[key] = datetime.time(int(t_tok[0]), int(t_tok[1]), int(t_tok[2]))
            else:
                self.params[key] = None
        elif res['param_type'] == '6':
            # Date
            if res['result'] == '1' and res['param_image']:
                # Request was successful
                self.params[key] = datetime.datetime.strptime(res['param_image'], '%d.%m.%Y')
            else:
                self.params[key] = None
        else:
            # Unexpected param type
            raise QuikLuaException(f'({self.class_code},{self.sec_code}):'
                                   f' getParamEx2() returned unknown param type: {param_ex_api_response}')


class HistoryCache:
    """In-memory history quote cache for speed up Quik historical data fetching"""
    def __init__(self, class_code: str, sec_code: str, interval: str, cache_update_min_interval_sec: float = 0.2):
        self.class_code = class_code
        self.sec_code = sec_code
        self.interval = interval
        self._data: Optional[pd.DataFrame] = None
        self._last_quote: Optional[pd.Timestamp] = None
        self.ds_uuid: Optional[str] = None
        self._lock = asyncio.Lock()
        self._last_update: Optional[datetime.datetime] = None
        self._cache_update_interval_sec = cache_update_min_interval_sec

    def process_history(self, quotes_df: pd.DataFrame) -> None:
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
    def data(self) -> Any:
        return self._data

    @property
    def last_bar_date(self) -> Any:
        return self._last_quote

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    @property
    def can_update(self) -> bool:
        if self._last_update is None:
            # Cache is new, let it update
            return True
        # Permit cache update only if self._cache_update_interval_sec passed
        return (datetime.datetime.now() - self._last_update).total_seconds() > self._cache_update_interval_sec
