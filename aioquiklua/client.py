from typing import Tuple, Dict, Any, List, Union
from .cache import HistoryCache, ParamCache, ParamWatcher
import zmq
import time
import zmq.asyncio
import asyncio
import datetime
import numpy as np
import pandas as pd
from .errors import *
import logging


def create_logger():
    log = logging.getLogger('aioquiklua')
    formatter = logging.Formatter(fmt=f'%(asctime)s [%(filename)s:%(lineno)s] %(levelname)s - %(message)s')
    handler_console = logging.StreamHandler()
    handler_console.setFormatter(formatter)
    log.setLevel(logging.DEBUG)
    log.addHandler(handler_console)
    return log


log = create_logger()
"""Default 'aioquiklua' logger"""


class QuikLuaClientBase:
    def __init__(self,
                 rpc_host,
                 data_host=None,
                 socket_timeout=100,
                 n_simultaneous_sockets=5,
                 history_backfill_interval_sec=10,
                 cache_min_update_sec=0.2,
                 params_poll_interval_sec=0.1,
                 verbosity=0,
                 logger=log
                 ):
        assert '127.0.0.1' in rpc_host or 'localhost' in rpc_host, f'Only localhost is allowed for RPC requests for security reasons, got {rpc_host}'
        self.verbosity = verbosity
        self.log = logger

        self.rpc_host = rpc_host
        self._data_host = data_host

        if self.verbosity > 0:
            self.log.debug(f'Connections params: RPC: {self.rpc_host} DATA: {self.data_host}')

        #
        # There is some limit of simultaneous sockets that Quick Lua RPC can handle
        # make asyncio.Semaphore() to control the queue of the self.rpc_call()
        self.n_simultaneous_sockets = n_simultaneous_sockets

        self.zmq_context = zmq.asyncio.Context()
        self.socket_timeout = socket_timeout

        self.history_backfill_interval_sec = history_backfill_interval_sec

        self._lock_rpc: asyncio.Semaphore = None   # this has to be initialized in initialize()
        self._lock_data: asyncio.Semaphore = None  # this has to be initialized in initialize()

        #
        # Historical cache
        #
        self._quote_cache: Dict[Tuple[str, str, str], HistoryCache] = {}
        self._quote_cache_min_update_sec = cache_min_update_sec

        self._params_cache: Dict[Tuple[str, str], ParamCache] = {}
        self._params_watcher = ParamWatcher()
        self._params_poll_interval = params_poll_interval_sec


        self._aio_background_tasks = []


        if self.verbosity > 1:
            self.log.debug(f'Quik client parameters:\n'
                           f'RPC Host: {self.rpc_host}\n'
                           f'DATA Host: {self.data_host}\n'
                           f'SocketTimeout: {self.socket_timeout}\n'
                           f'History Backfill Min Interval: {self.history_backfill_interval_sec}\n'
                           f'N simultaneous sockets: {self.n_simultaneous_sockets}\n'
                           f'Quote Cache Min Update sec: {self._quote_cache_min_update_sec}\n')

        self._is_shutting_down = False

    @property
    def data_host(self):
        if self._data_host is None:
            return self.rpc_host
        else:
            return self._data_host

    def get_lock(self, ltype: str):
        if self._lock_rpc is None:
            raise RuntimeError(f'Not initialized properly, you must call self.initialize() to setup QuikLuaClientBase')

        if self.data_host is None or self.data_host == self.rpc_host:
            return self._lock_rpc
        else:
            if ltype == 'rpc':
                return self._lock_rpc
            elif ltype == 'data':
                return self._lock_data
            else:
                raise NotImplementedError('Unknown lock type')

    async def params_subscribe(self, class_code: str, sec_code: str, update_interval_sec: Union[List[float], float], params_list: List[str]):
        """
        Requests Quik params subscription for unique combination of (class_code, sec_code) and initializes params cache.

        Notes:
            - Only one subscription per (class_code, sec_code) allowed, extend params_list if you need more fields
            - params_list names casted to lower case!
            - Method can support any instrument type, including options
            - Values of the params are casted to float, str, datetime.datetime, datetime.time (depending on type returned by Quik Lua API)
            - Missing or currently unfilled values may be None (for objects) or float('nan') for numeric
            - The client will take care of data updates under the hood, just call self.params_get(class_code: str, sec_code: str) to get most recent data

        :param class_code: Quik class code
        :param sec_code: Quik sec code
        :param update_interval_sec: poll interval for parameter, or maybe a list of number per each element in `params_list`
        :param params_list: a list of parameter to track (case insensitive, but casted to lower case later!), e.g. ['STATUS', 'lotsize', 'BID', 'offer']
                 # Unofficial list from https://forum.quik.ru/forum10/topic6280/
                 # It may be outdated, use DDE export to Excel to get valid values
                 STATUS                  STRING   Статус
                 LOTSIZE                 NUMERIC  Размер лота
                 BID                     NUMERIC  Лучшая цена спроса
                 BIDDEPTH                NUMERIC  Спрос по лучшей цене
                 BIDDEPTHT               NUMERIC  Суммарный спрос
                 NUMBIDS                 NUMERIC  Количество заявок на покупку
                 OFFER                   NUMERIC  Лучшая цена предложения
                 OFFERDEPTH              NUMERIC  Предложение по лучшей цене
                 OFFERDEPTHT             NUMERIC  Суммарное предложение
                 NUMOFFERS               NUMERIC  Количество заявок на продажу
                 OPEN                    NUMERIC  Цена открытия
                 HIGH                    NUMERIC  Максимальная цена сделки
                 LOW                     NUMERIC  Минимальная цена сделки
                 LAST                    NUMERIC  Цена последней сделки
                 CHANGE                  NUMERIC  Разница цены последней к предыдущей сессии
                 QTY                     NUMERIC  Количество бумаг в последней сделке
                 TIME                    STRING   Время последней сделки
                 VOLTODAY                NUMERIC  Количество бумаг в обезличенных сделках
                 VALTODAY                NUMERIC  Оборот в деньгах
                 TRADINGSTATUS           STRING   Состояние сессии
                 VALUE                   NUMERIC  Оборот в деньгах последней сделки
                 WAPRICE                 NUMERIC  Средневзвешенная цена
                 HIGHBID                 NUMERIC  Лучшая цена спроса сегодня
                 LOWOFFER                NUMERIC  Лучшая цена предложения сегодня
                 NUMTRADES               NUMERIC  Количество сделок за сегодня
                 PREVPRICE               NUMERIC  Цена закрытия
                 PREVWAPRICE             NUMERIC  Предыдущая оценка
                 CLOSEPRICE              NUMERIC  Цена периода закрытия
                 LASTCHANGE              NUMERIC  % изменения от закрытия
                 PRIMARYDIST             STRING   Размещение
                 ACCRUEDINT              NUMERIC  Накопленный купонный доход
                 YIELD                   NUMERIC  Доходность последней сделки
                 COUPONVALUE             NUMERIC  Размер купона
                 YIELDATPREVWAPRICE      NUMERIC  Доходность по предыдущей оценке
                 YIELDATWAPRICE          NUMERIC  Доходность по оценке
                 PRICEMINUSPREVWAPRICE   NUMERIC  Разница цены последней к предыдущей оценке
                 CLOSEYIELD              NUMERIC  Доходность закрытия
                 CURRENTVALUE            NUMERIC  Текущее значение индексов Московской Биржи
                 LASTVALUE               NUMERIC  Значение индексов Московской Биржи на закрытие предыдущего дня
                 LASTTOPREVSTLPRC        NUMERIC  Разница цены последней к предыдущей сессии
                 PREVSETTLEPRICE         NUMERIC  Предыдущая расчетная цена
                 PRICEMVTLIMIT           NUMERIC  Лимит изменения цены
                 PRICEMVTLIMITT1         NUMERIC  Лимит изменения цены T1
                 MAXOUTVOLUME            NUMERIC  Лимит объема активных заявок (в контрактах)
                 PRICEMAX                NUMERIC  Максимально возможная цена
                 PRICEMIN                NUMERIC  Минимально возможная цена
                 NEGVALTODAY             NUMERIC  Оборот внесистемных в деньгах
                 NEGNUMTRADES            NUMERIC  Количество внесистемных сделок за сегодня
                 NUMCONTRACTS            NUMERIC  Количество открытых позиций
                 CLOSETIME               STRING   Время закрытия предыдущих торгов (для индексов РТС)
                 OPENVAL                 NUMERIC  Значение индекса РТС на момент открытия торгов
                 CHNGOPEN                NUMERIC  Изменение текущего индекса РТС по сравнению со значением открытия
                 CHNGCLOSE               NUMERIC  Изменение текущего индекса РТС по сравнению со значением закрытия
                 BUYDEPO                 NUMERIC  Гарантийное обеспечение продавца
                 SELLDEPO                NUMERIC  Гарантийное обеспечение покупателя
                 CHANGETIME              STRING   Время последнего изменения
                 SELLPROFIT              NUMERIC  Доходность продажи
                 BUYPROFIT               NUMERIC  Доходность покупки
                 TRADECHANGE             NUMERIC  Разница цены последней к предыдущей сделки (FORTS, ФБ СПБ, СПВБ)
                 FACEVALUE               NUMERIC  Номинал (для бумаг СПВБ)
                 MARKETPRICE             NUMERIC  Рыночная цена вчера
                 MARKETPRICETODAY        NUMERIC  Рыночная цена
                 NEXTCOUPON              NUMERIC  Дата выплаты купона
                 BUYBACKPRICE            NUMERIC  Цена оферты
                 BUYBACKDATE             NUMERIC  Дата оферты
                 ISSUESIZE               NUMERIC  Объем обращения
                 PREVDATE                NUMERIC  Дата предыдущего торгового дня
                 DURATION                NUMERIC  Дюрация
                 LOPENPRICE              NUMERIC  Официальная цена открытия
                 LCURRENTPRICE           NUMERIC  Официальная текущая цена
                 LCLOSEPRICE             NUMERIC  Официальная цена закрытия
                 QUOTEBASIS              STRING   Тип цены
                 PREVADMITTEDQUOT        NUMERIC  Признаваемая котировка предыдущего дня
                 LASTBID                 NUMERIC  Лучшая спрос на момент завершения периода торгов
                 LASTOFFER               NUMERIC  Лучшее предложение на момент завершения торгов
                 PREVLEGALCLOSEPR        NUMERIC  Цена закрытия предыдущего дня
                 COUPONPERIOD            NUMERIC  Длительность купона
                 MARKETPRICE2            NUMERIC  Рыночная цена 2
                 ADMITTEDQUOTE           NUMERIC  Признаваемая котировка
                 BGOP                    NUMERIC  БГО по покрытым позициям
                 BGONP                   NUMERIC  БГО по непокрытым позициям
                 STRIKE                  NUMERIC  Цена страйк
                 STEPPRICET              NUMERIC  Стоимость шага цены
                 STEPPRICE               NUMERIC  Стоимость шага цены (для новых контрактов FORTS и RTS Standard)
                 SETTLEPRICE             NUMERIC  Расчетная цена
                 OPTIONTYPE              STRING   Тип опциона
                 OPTIONBASE              STRING   Базовый актив
                 VOLATILITY              NUMERIC  Волатильность опциона
                 THEORPRICE              NUMERIC  Теоретическая цена
                 PERCENTRATE             NUMERIC  Агрегированная ставка
                 ISPERCENT               STRING   Тип цены фьючерса
                 CLSTATE                 STRING   Статус клиринга
                 CLPRICE                 NUMERIC  Котировка последнего клиринга
                 STARTTIME               STRING   Начало основной сессии
                 ENDTIME                 STRING   Окончание основной сессии
                 EVNSTARTTIME            STRING   Начало вечерней сессии
                 EVNENDTIME              STRING   Окончание вечерней сессии
                 MONSTARTTIME            STRING   Начало утренней сессии
                 MONENDTIME              STRING   Окончание утренней сессии
                 CURSTEPPRICE            STRING   Валюта шага цены
                 REALVMPRICE             NUMERIC  Текущая рыночная котировка
                 MARG                    STRING   Маржируемый
                 EXPDATE                 NUMERIC  Дата исполнения инструмента
                 CROSSRATE               NUMERIC  Курс
                 BASEPRICE               NUMERIC  Базовый курс
                 HIGHVAL                 NUMERIC  Максимальное значение (RTSIND)
                 LOWVAL                  NUMERIC  Минимальное значение (RTSIND)
                 ICHANGE                 NUMERIC  Изменение (RTSIND)
                 IOPEN                   NUMERIC  Значение на момент открытия (RTSIND)
                 PCHANGE                 NUMERIC  Процент изменения (RTSIND)
                 OPENPERIODPRICE         NUMERIC  Цена предторгового периода
                 MIN_CURR_LAST           NUMERIC  Минимальная текущая цена
                 SETTLECODE              STRING   Код расчетов по умолчанию
                 STEPPRICECL             DOUBLE   Стоимость шага цены для клиринга
                 STEPPRICEPRCL           DOUBLE   Стоимость шага цены для промклиринга
                 MIN_CURR_LAST_TI        STRING   Время изменения минимальной текущей цены
                 PREVLOTSIZE             DOUBLE   Предыдущее значение размера лота
                 LOTSIZECHANGEDAT        DOUBLE   Дата последнего изменения размера лота
                 CLOSING_AUCTION_PRICE   NUMERIC  Цена послеторгового аукциона
                 CLOSING_AUCTION_VOLUME  NUMERIC  Количество в сделках послеторгового аукциона
                 LONGNAME                STRING   Полное название бумаги
                 SHORTNAME               STRING   Краткое название бумаги
                 CODE                    STRING   Код бумаги
                 CLASSNAME               STRING   Название класса
                 CLASS_CODE              STRING   Код класса
                 TRADE_DATE_CODE         DOUBLE   Дата торгов
                 MAT_DATE                DOUBLE   Дата погашения
                 DAYS_TO_MAT_DATE        DOUBLE   Число дней до погашения
                 SEC_FACE_VALUE          DOUBLE   Номинал бумаги
                 SEC_FACE_UNIT           STRING   Валюта номинала
                 SEC_SCALE               DOUBLE   Точность цены
                 SEC_PRICE_STEP          DOUBLE   Минимальный шаг цены
                 SECTYPE                 STRING   Тип инструмента

        :raises QuikLuaException: if param name is not valid or not applicable to instrument (for example, volatility for future contract)

        :return: Nothing
        """
        if self._is_shutting_down:
            raise asyncio.CancelledError()

        if (class_code, sec_code) in self._params_cache:
            raise QuikLuaException(f'{(class_code, sec_code)} already exists in subscription cache, duplicated subscriptions are not allowed')

        if not isinstance(update_interval_sec, (list, tuple, float, int)):
            raise QuikLuaException(f'{(class_code, sec_code)} update_interval_sec must be list, tuple, or float/int')

        if isinstance(update_interval_sec, (list, tuple)):
            if len(update_interval_sec) != len(params_list):
                raise QuikLuaException(f'{(class_code, sec_code)} update_interval_sec list must have equal length with params_list')
        else:
            if update_interval_sec <= 0:
                raise QuikLuaException(f'{(class_code, sec_code)} update_interval_sec must be positive')

        # self.get_lock(): Do not allow creation of more than self.n_simultaneous_sockets
        params_to_watch = []
        async with self.get_lock('data'):
            try:
                _socket = self.zmq_context.socket(zmq.REQ)
                _socket.setsockopt(zmq.RCVTIMEO, self.socket_timeout)   # Make raising exceptions when receiving socket timeout or not exists
                _socket.setsockopt(zmq.LINGER, 1000)                    # Free socket at socket.close timeout
                _socket.connect(self.data_host)
                if self.verbosity > 1:
                    self.log.debug(f'params_subscribe({class_code}, {sec_code}, {params_list})')

                # Request params
                for param in params_list:
                    await self._socket_send_receive_json(_socket, 'ParamRequest', class_code=class_code, sec_code=sec_code, db_name=param)

                # Fill initial values of the params
                cache = ParamCache(class_code, sec_code, params_list)
                for i, param in enumerate(params_list):
                    param_ex_api_response = await self._socket_send_receive_json(_socket, 'getParamEx2', class_code=class_code, sec_code=sec_code, param_name=param)
                    cache.process_param(param, param_ex_api_response)
                    if isinstance(update_interval_sec, (list, tuple)):
                        assert isinstance(update_interval_sec[i], (float, int, np.float, np.int)), f'update_interval_sec: Expected float got {update_interval_sec[i]}'
                        params_to_watch.append((class_code, sec_code, param, update_interval_sec[i]))
                    else:
                        params_to_watch.append((class_code, sec_code, param, float(update_interval_sec)))

                self._params_cache[(class_code, sec_code)] = cache

                if self.verbosity > 1:
                    self.log.debug(f'params_subscribe({class_code}, {sec_code}) -- initial values -- {cache.params}')
            except zmq.ZMQError as exc:
                raise QuikLuaConnectionException(repr(exc))
            finally:
                try:
                    # Cleanup
                    _socket.close()
                except:
                    pass

        # Add new params to watcher
        if params_to_watch:
            async with self._params_watcher.lock:
                self._params_watcher.subscribed(params_to_watch)

    async def params_unsubscribe(self, class_code: str, sec_code: str):
        """
        Unsubscribe all params for (class_code, sec_code)
        :param class_code:
        :param sec_code:
        :return:
        """

        if (class_code, sec_code) not in self._params_cache:
            raise QuikLuaException(f'{(class_code, sec_code)} is not subscribed')

        cache = self._params_cache[(class_code, sec_code)]
        params_to_unwatch = [(class_code, sec_code, param) for param in cache.params.keys()]

        if params_to_unwatch:
            async with self._params_watcher.lock:
                self._params_watcher.unsubscribed(params_to_unwatch)

        # self.get_lock(): Do not allow creation of more than self.n_simultaneous_sockets
        async with self.get_lock('data'):
            try:
                _socket = self.zmq_context.socket(zmq.REQ)
                _socket.setsockopt(zmq.RCVTIMEO, self.socket_timeout)   # Make raising exceptions when receiving socket timeout or not exists
                _socket.setsockopt(zmq.LINGER, 1000)                    # Free socket at socket.close timeout
                _socket.connect(self.data_host)

                if self.verbosity > 1:
                    self.log.debug(f'params_unsubscribe({class_code}, {sec_code})')

                # Request params
                for param in cache.params.keys():
                    await self._socket_send_receive_json(_socket, 'CancelParamRequest', class_code=class_code, sec_code=sec_code, db_name=param)

                del self._params_cache[(class_code, sec_code)]
            except zmq.ZMQError as exc:
                raise QuikLuaConnectionException(repr(exc))
            finally:
                try:
                    # Cleanup
                    _socket.close()
                except:
                    pass

    async def _params_watch_task(self):
        """
        AsyncIO task for updating price parameters
        :return:
        """
        while True:
            _socket = None
            await asyncio.sleep(self._params_poll_interval)
            b_time = time.time()

            if self._is_shutting_down:
                # Close task
                raise asyncio.CancelledError()

            try:
                async with self._params_watcher.lock:
                    candidates = self._params_watcher.get_update_candidates()
                    if len(candidates) > 0:
                        _socket = self.zmq_context.socket(zmq.REQ)
                        _socket.setsockopt(zmq.RCVTIMEO, self.socket_timeout)  # Make raising exceptions when receiving socket timeout or not exists
                        _socket.setsockopt(zmq.LINGER, 1000)  # Free socket at socket.close timeout
                        _socket.connect(self.data_host)

                        for (class_code, sec_code, param) in candidates['key']:
                            cache = self._params_cache[(class_code, sec_code)]
                            rpc_result = await self._socket_send_receive_json(_socket, 'getParamEx2', class_code=class_code, sec_code=sec_code, param_name=param)
                            cache.process_param(param, rpc_result)

                        self._params_watcher.set_candidate_updates(candidates)
                        if self.verbosity > 2:
                            self.log.debug(f'#{len(candidates)} instruments params updated in {time.time()-b_time}sec')
            except:
                raise
            finally:
                if _socket:
                    _socket.close()
                    _socket = None


    def params_get(self, class_code: str, sec_code: str) -> Dict[str, Any]:
        """
        Get (class_code, sec_code) that tracked by self.params_subscribe(...)

        :param class_code:
        :param sec_code:
        :raises QuikLuaException: if (class_code, sec_code) is not subscribed
        :return: a dictionary with all params
            Example:
            > await qclient.params_subscribe('SPBFUT', 'RIH1', ['bid', 'offer', 'last', 'MAT_DATE', 'time', 'STEPPRICET'])
            > qclient.params_get('SPBFUT', 'RIH1')

             {'bid': 152420.0,
             'offer': 152500.0,
             'last': 152440.0,
             'mat_date': datetime.datetime(2021, 3, 18, 0, 0),
             'time': datetime.time(23, 49, 59),
             'steppricet': 14.69954}
        """
        if self._is_shutting_down:
            raise asyncio.CancelledError()

        params = self._params_cache.get((class_code, sec_code))
        if params is None:
            raise QuikLuaException(f'{(class_code, sec_code)} not found in params, call self.params_subscribe() first')
        return params.params

    async def initialize(self):
        if self._is_shutting_down:
            raise asyncio.CancelledError()
        if self.verbosity > 1:
            self.log.debug(f'Initializing Quik LUA Client')
        if self._lock_rpc is not None:
            raise RuntimeError(f'Initialize must be called only once!')

        self._lock_rpc = asyncio.Semaphore(self.n_simultaneous_sockets)
        self._lock_data = asyncio.Semaphore(self.n_simultaneous_sockets)

        # Make params update task run in background
        # IMPORTANT: it may raise exceptions, but you must call self.heartbeat() function to check its status
        self._aio_background_tasks.append(asyncio.create_task(self._params_watch_task()))

    async def main(self):
        """
        Main client entry point, also does some low level initialization of the client app.

        Consider to override this method in child class and call `await super().main()` in it

        :return:
        """
        if self._is_shutting_down:
            raise asyncio.CancelledError()

        # Send a heartbeat to test the RPC connection!
        await self.heartbeat()

    async def shutdown(self):
        """
        Shutdowns Quik Lua Socket connection and unsubscribes the data

        You MUST call this method and the application shutdown to prevent memory leaks of the Quik terminal

        :return: Nothing
        """
        # Make other parallel tasks to exit
        self._is_shutting_down = True

        #
        # Free existing Quik Datasources
        #
        if self._quote_cache:
            if self.verbosity > 0:
                self.log.debug('Cleanup quik history caches')

            # self._quote_cache may change in some rare cases, which prevents us from closing data source
            all_cache = list(self._quote_cache.items())
            for key, cache in all_cache:
                if cache.ds_uuid:
                    if self.verbosity > 1:
                        self.log.debug(f'datasource.Close: {key} {cache.ds_uuid}')
                    await self.rpc_call("datasource.Close", datasource_uuid=cache.ds_uuid)

        if self._params_cache:
            if self.verbosity > 0:
                self.log.debug('Cleanup quik param cache')

            all_subscriptions = list(self._params_cache.keys())
            for (class_code, sec_code) in all_subscriptions:
                await self.params_unsubscribe(class_code, sec_code)

        self.zmq_context.destroy()

    async def heartbeat(self):
        """
        Checks LUA socket connection and last Quik record time

        Also checks health status of all client background tasks like params update routines.

        :return: LUA Func getInfoParam('LASTRECORDTIME') or raises QuikLuaConnectionException() if socket disconnected
        """
        if self._is_shutting_down:
            raise asyncio.CancelledError()
        if self.verbosity > 1:
            self.log.debug('Heartbeat call sent')

        if self._lock_rpc is None:
            raise RuntimeError(f'Client not initialized properly, you must call self.initialize() first')

        #
        # Check background tasks if they raised any unhandled exceptions, then re-raise
        #
        for task_future in self._aio_background_tasks:
            try:
                watcher_exc = task_future.exception()
                if isinstance(watcher_exc, Exception):
                    raise task_future.exception()
            except asyncio.InvalidStateError:
                # All good, task is running
                pass

        return await self.rpc_call('getInfoParam', param_name='LASTRECORDTIME')

    async def get_price_history(self, class_code: str, sec_code: str, interval: str, use_caching=True, copy=True) -> pd.DataFrame:
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
        :param copy: returns a copy of a data cache
        :exception: QuikLuaNoHistoryException - if no history found or backfill is too long (see self.history_backfill_interval_sec param)
        :return: pd.DataFrame(['o', 'h', 'l', 'c', 'v']) with DateTime Index (TZ in Moscow time, but tz-naive)
        """

        if self._is_shutting_down:
            raise asyncio.CancelledError()

        # self.get_lock(): Do not allow creation of more than self.n_simultaneous_sockets
        async with self.get_lock('data'):
            try:
                _socket = self.zmq_context.socket(zmq.REQ)
                _socket.setsockopt(zmq.RCVTIMEO, self.socket_timeout)  # Make raising exceptions when receiving socket timeout or not exists
                _socket.setsockopt(zmq.LINGER, 1000)  # Free socket at socket.close timeout
                _socket.connect(self.data_host)

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
                        if self.verbosity > 1:
                            self.log.debug(f'Quote cache hit for {(class_code, sec_code, interval)}')
                        if copy:
                            return cache.data.copy()
                        else:
                            return cache.data

                    if cache.ds_uuid is None:
                        # Cache is not initialized, create a new DataSource in Quik
                        response = await self._socket_send_receive_json(_socket,
                                                                        "datasource.CreateDataSource",
                                                                        **{"class_code": class_code, "sec_code": sec_code, "interval": interval, "param": ""})

                        ds_uuid = response['datasource_uuid']
                        cache.ds_uuid = ds_uuid
                        if self.verbosity > 1:
                            self.log.debug(f'Created DataSource: {(class_code, sec_code, interval)} uuid: {ds_uuid}')
                    else:
                        # re-using Quik uuid from datasource.CreateDataSource with the same (class_code, sec_code, interval) combination
                        ds_uuid = cache.ds_uuid

                    n_tries = 0
                    time_begin = time.time()
                    response_size = {'value': 0}
                    while response_size['value'] == 0:
                        if self._is_shutting_down:
                            raise asyncio.CancelledError()

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
                    if self.verbosity > 2:
                        self.log.debug(f'Got the initial data after: {time.time() - time_begin:0.2f}sec ({(class_code, sec_code, interval)})')

                    result = []
                    time_begin = time.time()
                    last_bar_date = cache.last_bar_date
                    for i in range(bar_count, 0, -1):
                        if self._is_shutting_down:
                            raise asyncio.CancelledError()
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
                            'o': float(candle_open),
                            'h': float(candle_high),
                            'l': float(candle_low),
                            'c': float(candle_close),
                            'v': float(candle_vol),
                        })

                    if not use_caching:
                        # Closing the datasource and fee resources, otherwise we should close it on exit
                        await self._socket_send_receive_json(_socket, "datasource.Close",  datasource_uuid=ds_uuid)

                    # Update history cache if applicable
                    quotes_df = pd.DataFrame(result).set_index('dt').sort_index()
                    if self.verbosity > 1:
                        self.log.debug(f'{sec_code} {len(quotes_df)} bars updated in cache')
                    cache.process_history(quotes_df)

                    if self.verbosity > 2:
                        self.log.debug(f'Historical quotes processed in {time.time() - time_begin:0.2f}sec ({(class_code, sec_code, interval)})')

                    # Return full history from cache
                    if copy:
                        return cache.data.copy()
                    else:
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

        if 'result' in response and not response['result'].get('is_error'):
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

        # self.get_lock(): Do not allow creation of more than self.n_simultaneous_sockets
        async with self.get_lock('rpc'):
            try:
                _socket = self.zmq_context.socket(zmq.REQ)
                _socket.setsockopt(zmq.RCVTIMEO, self.socket_timeout)   # Make raising exceptions when receiving socket timeout or not exists
                _socket.setsockopt(zmq.LINGER, 1000)                    # Free socket at socket.close timeout
                _socket.connect(self.rpc_host)

                rpc_result = await self._socket_send_receive_json(_socket, rpc_func, **rpc_args)
                if self.verbosity == 2:
                    self.log.debug(f'rpc_call: {rpc_func}({rpc_args})')
                elif self.verbosity > 2:
                    self.log.debug(f'rpc_call: {rpc_func}({rpc_args}) -> {rpc_result}')

                return rpc_result
            except zmq.ZMQError as exc:
                raise QuikLuaConnectionException(repr(exc))
            finally:
                try:
                    # Cleanup
                    _socket.close()
                except:
                    pass
