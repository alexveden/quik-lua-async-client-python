import asyncio
import concurrent.futures

import zmq
import zmq.asyncio
import json
from collections import defaultdict
import time

from .errors import *


class ZMQSocketPoolAsync:
    def __init__(self, rpc_host, socket_timeout=100, n_sockets=5, n_retries=2):
        self.zmq_context = zmq.Context.instance()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=n_sockets)
        self.socket_timeout = socket_timeout
        self.n_sockets = n_sockets
        self._socket_cache = [None for i in range(n_sockets)]
        self._sockets_in_use = [None for i in range(n_sockets)]
        self._socket_retries = n_retries
        self._lock = asyncio.Semaphore(self.n_sockets)
        self.rpc_host = rpc_host
        self._loop = asyncio.get_running_loop()
        self._stats_rpc_errors = 0
        self._stats_socket_errors = 0
        self._stats_rpc_calls = defaultdict(int)
        self._stats_rpc_total_time = 0.0
        self._stats_rpc_total_count = 0.0

    def stats(self):
        return dict(
            rpc_call_count = self._stats_rpc_total_count,
            rpc_avg_roundtrip=-1 if self._stats_rpc_total_count == 0 else self._stats_rpc_total_time / self._stats_rpc_total_count,
            rpc_call_functions= dict(self._stats_rpc_calls),
            rpc_error_count = self._stats_rpc_errors,
            socket_error_count = self._stats_socket_errors,
            )

    def stats_reset(self):
        self._stats_rpc_errors = 0
        self._stats_socket_errors = 0
        self._stats_rpc_calls = defaultdict(int)
        self._stats_rpc_total_time = 0.0
        self._stats_rpc_total_count = 0.0


    def _acquire_socket(self):
        for i in range(self.n_sockets):
            if self._sockets_in_use[i] is None:
                if self._socket_cache[i] is None:
                    self._init_socket(i)
                self._sockets_in_use[i] = self._socket_cache[i]
                return self._socket_cache[i], i
        raise RuntimeError(f'Socket number overflow, more requests than capacity')

    def _release_socket(self, idx):
        self._sockets_in_use[idx] = None

    def _init_socket(self, idx):
        _socket = self.zmq_context.socket(zmq.REQ)
        _socket.connect(self.rpc_host)
        self._socket_cache[idx] = _socket
        return _socket

    def _close_socket(self, idx):
        socket = self._socket_cache[idx]
        socket.setsockopt(zmq.LINGER, 0)
        socket.close()
        self._socket_cache[idx] = None

    def send_receive(self, socket, req):
        time_start = time.time()

        _send_result = socket.send_json(req)

        if ((socket.poll(self.socket_timeout)) & zmq.POLLIN) != 0:
            response_bytes = socket.recv(0)
            # response = json.loads(response_bytes.decode('cp1251'))
            try:
                response = json.loads(response_bytes.decode('utf-8'))
            except UnicodeDecodeError:
                response = json.loads(response_bytes.decode('cp1251'))

            call_duration = time.time() - time_start
            self._stats_rpc_total_count += 1
            self._stats_rpc_total_time += call_duration

            if 'result' in response and not response['result'].get('is_error'):
                return True, response['result']
            else:
                self._stats_rpc_errors += 1
                raise QuikLuaException(f"{req} error: {response.get('error', response)}")
        else:
            return False, None

    async def rpc_call(self, rpc_func, **rpc_args):
        """
        Utility shortcut function for sending requests to RPC sockets and parsing response
        """
        async with self._lock:
            retries_left = self._socket_retries
            socket, idx = self._acquire_socket()
            try:
                #
                # Using lazy pirate pattern if socket is dead
                # https://zguide.zeromq.org/docs/chapter4/
                #
                while True:
                    req = {'method': rpc_func}
                    if rpc_args:
                        # Pass optional arguments
                        req['args'] = rpc_args
                    
                    self._stats_rpc_calls[rpc_func] += 1

                    try:
                        # Using thread pool is about 3x faster than use `await zmq.asyncio.Sockets`
                        is_success, result = await self._loop.run_in_executor(self.thread_pool, self.send_receive, socket, req)
                    except zmq.ZMQError:
                        # Retry again
                        is_success, result = False, None

                    if is_success:
                        return result

                    retries_left -= 1
                    self._stats_socket_errors += 1

                    # Socket is confused. Close and remove it.
                    self._close_socket(idx)

                    if retries_left <= 0:
                        raise QuikLuaConnectionException(f"Server seems to be offline, abandoning")

                    socket = self._init_socket(idx)
            finally:
                self._release_socket(idx)

    def close(self):
        for i, socket in enumerate(self._socket_cache):
            if socket:
                self._close_socket(i)
