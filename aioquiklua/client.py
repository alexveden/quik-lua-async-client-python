from typing import Tuple, Dict
from .cache import HistoryCache
import zmq
import time
import zmq.asyncio
import asyncio
import datetime
import pandas as pd
from .errors import *


class QuikLuaClientBase:
    def __init__(self,
                 rpc_host,
                 pub_host,
                 socket_timeout=100,
                 n_simultaneous_sockets=5,
                 history_backfill_interval_sec=10,
                 cache_min_update_sec=0.2,
                 ):
        assert '127.0.0.1' in rpc_host or 'localhost' in rpc_host, f'Only localhost is allowed for RPC requests for security reasons, got {rpc_host}'

        self.rpc_host = rpc_host
        self.pub_host = pub_host
        self.zmq_context = zmq.asyncio.Context()
        self.socket_timeout = socket_timeout

        self.history_backfill_interval_sec = history_backfill_interval_sec

        #
        # There is some limit of simultaneous sockets that Quick Lua RPC can handle
        # make asyncio.Semaphore() to control the queue of the self.rpc_call()
        self.n_simultaneous_sockets = n_simultaneous_sockets
        self._lock_rpc: asyncio.Semaphore = None  # this has to be initialized in main()

        #
        # Historical cache
        #
        self._quote_cache: Dict[Tuple[str, str, str], HistoryCache] = {}
        self._quote_cache_min_update_sec = cache_min_update_sec

    async def main(self):
        """
        Main client entry point, also does some low level initialization of the client app.

        Consider to override this method in child class and call `await super().main()` in it

        :return:
        """
        self._lock_rpc = asyncio.Semaphore(self.n_simultaneous_sockets)

        # Send a heartbeat to test the RPC connection!
        await self.heartbeat()

    async def shutdown(self):
        """
        Shutdowns Quik Lua Socket connection and unsubscribes the data

        You MUST call this method and the application shutdown to prevent memory leaks of the Quik terminal

        :return: Nothing
        """
        #
        # Free existing Quik Datasources
        #
        if self._quote_cache:
            print('Cleanup quik history caches')
            for key, cache in self._quote_cache.items():
                if cache.ds_uuid:
                    print(f'datasource.Close: {key} {cache.ds_uuid}')
                    await self.rpc_call("datasource.Close", datasource_uuid=cache.ds_uuid)

        self.zmq_context.destroy()

    async def heartbeat(self):
        """
        Checks LUA socket connection and last Quik record time

        :return: LUA Func getInfoParam('LASTRECORDTIME') or raises QuikLuaConnectionException() if socket disconnected
        """
        return await self.rpc_call('getInfoParam', param_name='LASTRECORDTIME')

    async def get_price_history(self, class_code: str, sec_code: str, interval: str, use_caching=True) -> pd.DataFrame:
        """
        Retrieve price history from Quik server, and use cache if applicable

        :param class_code: instrument class (example: 'SPBFUT')
        :param sec_code: sec_code (example: 'SiU9')
        :param interval:  string interval parameter
            'INTERVAL_TICK'
            'INTERVAL_M1'
            'INTERVAL_M<N>' -  <N> Minutes in [1,2,3,4,5,6,10,15,30]
            'INTERVAL_H<N>' - <N> Hours in [1,2,4]
            'INTERVAL_D1' - daily
            'INTERVAL_W1' - weekly
            'INTERVAL_MN1' - monthly
        :param use_caching: use in-memory cache and update only most recent data since last historical request
        :exception: QuikLuaNoHistoryException - if no history found or backfill is too long (see self.history_backfill_interval_sec param)
        :return: pd.DataFrame(['o', 'h', 'l', 'c', 'v']) with DateTime Index (TZ in Moscow time, but tz-naive)
        """
        if self._lock_rpc is None:
            raise RuntimeError(f'Not initialized properly, you must call super().main() to setup QuikLuaClientBase')

        # self._lock_rpc: Do not allow creation of more than self.n_simultaneous_sockets
        async with self._lock_rpc:
            try:
                _socket = self.zmq_context.socket(zmq.REQ)
                _socket.setsockopt(zmq.RCVTIMEO, self.socket_timeout)  # Make raising exceptions when receiving socket timeout or not exists
                _socket.setsockopt(zmq.LINGER, 1000)  # Free socket at socket.close timeout
                _socket.connect(self.rpc_host)

                if use_caching:
                    cache_key = (class_code, sec_code, interval)
                    if cache_key in self._quote_cache:
                        cache = self._quote_cache[cache_key]
                    else:
                        cache = HistoryCache(class_code, sec_code, interval, cache_update_min_interval_sec=self._quote_cache_min_update_sec)
                        self._quote_cache[cache_key] = cache
                else:
                    # No Caching! Create a temporary cache, just for this call
                    cache = HistoryCache(class_code, sec_code, interval)

                async with cache.lock:
                    # Allow only one cache update for (class_code, sec_code, interval) combination
                    if not cache.can_update:
                        # Cache update is too frequent, use in-memory data
                        return cache.data

                    if cache.ds_uuid is None:
                        # Cache is not initialized, create a new DataSource in Quik
                        response = await self._socket_send_receive_json(_socket,
                                                                        "datasource.CreateDataSource",
                                                                        **{"class_code": class_code, "sec_code": sec_code, "interval": interval, "param": ""})

                        ds_uuid = response['datasource_uuid']
                        cache.ds_uuid = ds_uuid
                    else:
                        # re-using Quik uuid from datasource.CreateDataSource with the same (class_code, sec_code, interval) combination
                        ds_uuid = cache.ds_uuid

                    n_tries = 0
                    time_begin = time.time()
                    response_size = {'value': 0}
                    while response_size['value'] == 0:
                        #
                        # It will take some time to load initial data from Quik server, and initially it returns zero side for some period of time,
                        #       just wait until the requested data becomes available, or maybe sec_code is incorrect then raise an exception
                        if time.time() - time_begin > self.history_backfill_interval_sec:
                            raise QuikLuaNoHistoryException(f'No history returned, backfill timeout > {self.history_backfill_interval_sec}sec and #{n_tries} tries')
                        response_size = await self._socket_send_receive_json(_socket,
                                                                             "datasource.Size",
                                                                             datasource_uuid=ds_uuid)
                        # Wait until quik backfills the data from the server, may take several seconds!
                        if response_size['value'] == 0:
                            #print('wait')
                            await asyncio.sleep(0.2)
                        else:
                            break
                        n_tries += 1
                    bar_count = response_size['value']
                    #print(f'Got the data after: {time.time() - time_begin:0.2f}sec')

                    result = []

                    last_bar_date = cache.last_bar_date
                    for i in range(bar_count, 0, -1):
                        candle_open = (await self._socket_send_receive_json(_socket, "datasource.O", datasource_uuid=ds_uuid, candle_index=i))['value']
                        candle_high = (await self._socket_send_receive_json(_socket, "datasource.H", datasource_uuid=ds_uuid, candle_index=i))['value']
                        candle_low = (await self._socket_send_receive_json(_socket, "datasource.L", datasource_uuid=ds_uuid, candle_index=i))['value']
                        candle_close = (await self._socket_send_receive_json(_socket, "datasource.C", datasource_uuid=ds_uuid, candle_index=i))['value']
                        candle_vol = (await self._socket_send_receive_json(_socket, "datasource.V", datasource_uuid=ds_uuid, candle_index=i))['value']
                        _dt = (await self._socket_send_receive_json(_socket, "datasource.T", datasource_uuid=ds_uuid, candle_index=i))['time']
                        bar_date = datetime.datetime(_dt['year'], _dt['month'], _dt['day'], _dt['hour'], _dt['min'], _dt['sec'], _dt['ms'] * 1000)

                        if bar_date < last_bar_date:
                            # Update only until last bar in cache (including it, to update the most recent bar)
                            break

                        result.append({
                            'dt': bar_date,
                            'o': candle_open,
                            'h': candle_high,
                            'l': candle_low,
                            'c': candle_close,
                            'v': candle_vol,
                        })

                    if not use_caching:
                        # Closing the datasource and fee resources, otherwise we should close it on exit
                        await self._socket_send_receive_json(_socket, "datasource.Close",  datasource_uuid=ds_uuid)

                    # Update history cache if applicable
                    quotes_df = pd.DataFrame(result).set_index('dt').sort_index()
                    print(f'{sec_code} {len(quotes_df)} bars updated in cache')
                    cache.process_history(quotes_df)

                    # Return full history from cache
                    return cache.data

            except zmq.ZMQError as exc:
                raise QuikLuaConnectionException(repr(exc))
            finally:
                try:
                    # Cleanup
                    _socket.close()
                except:
                    pass

    @staticmethod
    async def _socket_send_receive_json(socket, rpc_func, **rpc_args):
        """
        Utility shortcut function for sending requests to RPC sockets and parsing response
        """
        req = {'method': rpc_func}
        if rpc_args:
            # Pass optional arguments
            req['args'] = rpc_args

        _send_result = await socket.send_json(req)
        response = await socket.recv_json()

        if 'result' in response:
            return response['result']
        else:
            raise QuikLuaException(f"{rpc_func} error: {response.get('error', response)}")

    async def rpc_call(self, rpc_func: str, **rpc_args) -> dict:
        """
        Sends RPC call to the Quick Lua endpoint

        Examples:
            await self.rpc_call('getScriptPath') -> returns {'script_path': 'C:\\QUIK\\lua\\quik-lua-rpc'}
            await self.rpc_call('message', message='Hello world', icon_type='WARNING') -> returns{'result': 1}
            await self.rpc_call('NotExistingLuaFunc') -> raises QuikLuaException: Code: 404 -- QLua-функция с именем 'NotExistingLuaFunc' не найдена.
        :param rpc_func: RPC function name
        :param rpc_args: any arbitrary set of arguments that passed to RPC function
        :raises:
            QuikLuaException - on function errors
            QuikLuaConnectionException - on ZeroMQ connectivity errors
        :return: dict with result
        """
        if self._lock_rpc is None:
            raise RuntimeError(f'Not initialized properly, you must call super().main() to setup QuikLuaClientBase')

        # self._lock_rpc: Do not allow creation of more than self.n_simultaneous_sockets
        async with self._lock_rpc:
            try:
                _socket = self.zmq_context.socket(zmq.REQ)
                _socket.setsockopt(zmq.RCVTIMEO, self.socket_timeout)   # Make raising exceptions when receiving socket timeout or not exists
                _socket.setsockopt(zmq.LINGER, 1000)                    # Free socket at socket.close timeout
                _socket.connect(self.rpc_host)

                return await self._socket_send_receive_json(_socket, rpc_func, **rpc_args)
            except zmq.ZMQError as exc:
                raise QuikLuaConnectionException(repr(exc))
            finally:
                try:
                    # Cleanup
                    _socket.close()
                except:
                    pass
