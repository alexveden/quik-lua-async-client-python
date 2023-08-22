import asyncio
import datetime as dtm
import logging
import time
from typing import Tuple, Dict, Any, List, Union, Optional, Callable

import pandas as pd
import zmq
import zmq.asyncio

from .cache import HistoryCache, ParamCache, ParamWatcher
from .errors import QuikLuaNoHistoryException, QuikLuaException, QuikLuaConnectionException
from .socket_pool import ZMQSocketPoolAsync


def create_logger() -> logging.Logger:
    log = logging.getLogger('aioquiklua')
    formatter = logging.Formatter(fmt='%(asctime)s [%(filename)s:%(lineno)s] %(levelname)s - %(message)s')
    handler_console = logging.StreamHandler()
    handler_console.setFormatter(formatter)
    log.setLevel(logging.DEBUG)
    log.addHandler(handler_console)
    return log


default_logger = create_logger()
"""Default 'aioquiklua' logger"""


class QuikLuaClientBase:
    def __init__(self,
                 rpc_host: str,
                 data_host: Optional[str] = None,
                 socket_timeout: int = 100,
                 n_simultaneous_sockets: int = 5,
                 history_backfill_interval_sec: int = 10,
                 cache_min_update_sec: float = 0.2,
                 params_poll_interval_sec: float = 0.1,
                 params_delay_timeout_sec: float = 60.0,
                 verbosity: int = 0,
                 logger: logging.Logger = default_logger,
                 event_host: Optional[str] = None,
                 event_list: Optional[List[str]] = None,
                 event_callback_coro: Optional[Callable] = None,
                 ):
        """
        Initializes Quik LUA RPC Async client

        :param rpc_host: RPC socket, as in `config.json` of Quik Lua RPC script (example: "tcp://localhost:5580")
        :param data_host: Alternative RPC socket, used only for data requests
        as in `config.json` of Quik Lua RPC script. If None it uses rpc_host value
        :param socket_timeout: initial socket connection timeout for RPC requests
        :param n_simultaneous_sockets: number of simultaneous socket connections for increased concurrency speed
        :param history_backfill_interval_sec: historical bars poll intervals
        :param cache_min_update_sec: quik current params update intervals
        :param params_poll_interval_sec: quik current params poll intervals
        :param params_delay_timeout_sec: minimal update interval between subscribed params (because on reconnection
        Quik keep sending outdated params, we need to check if params were updated, and resubscribe)
        :param verbosity: debug information level 0-silent, higher the value the more information is printed to the log
        :param logger: logger instance
        :param event_host: PUB socket, as in `config.json` of Quik Lua RPC script (example: "tcp://localhost:5581")
        :param event_list: list of events to filter, by default handles all events
        :param event_callback_coro: external coroutine function for event handling,
        or override on_new_event() in child class
        """
        self.verbosity = verbosity
        self.log = logger

        self.rpc_host = rpc_host
        self._data_host = data_host
        self._event_host = event_host
        self._event_filter = {e.lower() for e in event_list} if event_list else None
        self._event_que: asyncio.Queue = None  # type: ignore
        self._event_callback_coro = event_callback_coro

        if self.verbosity > 0:
            self.log.debug('Connections params: RPC: %s DATA: %s EVENT: %s',
                           self.rpc_host, self._data_host, self._event_host)

        #
        # There is some limit of simultaneous sockets that Quick Lua RPC can handle
        # make asyncio.Semaphore() to control the queue of the self.rpc_call()
        self.socket_timeout = socket_timeout
        self.n_simultaneous_sockets = n_simultaneous_sockets
        self.zmq_context: zmq.asyncio.Context = zmq.asyncio.Context()
        self.zmq_pool_rpc: ZMQSocketPoolAsync = None  # type: ignore
        self.zmq_pool_data: ZMQSocketPoolAsync = None  # type: ignore

        self.history_backfill_interval_sec = history_backfill_interval_sec
        self._lock_event: asyncio.Lock = asyncio.Lock()

        #
        # Historical cache
        #
        self._quote_cache: Dict[Tuple[str, str, str], HistoryCache] = {}
        self._quote_cache_min_update_sec = cache_min_update_sec

        self._params_cache: Dict[Tuple[str, str], ParamCache] = {}
        self._params_watcher = ParamWatcher()
        self._params_poll_interval = params_poll_interval_sec
        self._params_delay_timeout_sec = params_delay_timeout_sec

        self._last_data_processed: Optional[dtm.datetime] = None
        self._last_event_processed: Optional[dtm.datetime] = None
        self._last_quote_processed: Optional[dtm.datetime] = None

        self._aio_background_tasks: List[Any] = []

        if self.verbosity > 1:
            self.log.debug('Quik client parameters:\n'
                           'RPC Host: %s\n'
                           'DATA Host: %s\n'
                           'SocketTimeout: %s\n'
                           'History Backfill Min Interval: %s\n'
                           'N simultaneous sockets: %s\n'
                           'Quote Cache Min Update sec: %s\n',
                           self.rpc_host,
                           self._data_host,
                           self.socket_timeout,
                           self.history_backfill_interval_sec,
                           self.n_simultaneous_sockets,
                           self._quote_cache_min_update_sec)

        self._is_shutting_down = False

    async def initialize(self) -> None:
        if self._is_shutting_down:
            raise asyncio.CancelledError()
        if self.verbosity > 1:
            self.log.debug('Initializing Quik LUA Client')

        self.zmq_pool_rpc = ZMQSocketPoolAsync(self.rpc_host,
                                               socket_timeout=self.socket_timeout,
                                               n_sockets=self.n_simultaneous_sockets,
                                               n_retries=2)

        if self._data_host is None:
            self.zmq_pool_data = self.zmq_pool_rpc
        else:
            self.zmq_pool_data = ZMQSocketPoolAsync(self._data_host,
                                                    socket_timeout=self.socket_timeout,
                                                    n_sockets=self.n_simultaneous_sockets,
                                                    n_retries=2)

        # We must pass current event loop to Queue,
        # to make it compatible with asyncio.create_task(self._events_dispatcher_task())
        # self._event_que = asyncio.Queue(loop=asyncio.get_running_loop())
        self._event_que = asyncio.Queue()

        # Make params update task run in background
        # IMPORTANT: it may raise exceptions, but you must call self.heartbeat() function to check its status
        self._aio_background_tasks.append(
            asyncio.create_task(self._params_watch_task(), name='aioquiklua-_params_watch_task'))
        self._aio_background_tasks.append(
            asyncio.create_task(self._events_watch_task(), name='aioquiklua-_events_watch_task'))
        self._aio_background_tasks.append(
            asyncio.create_task(self._events_dispatcher_task(), name='aioquiklua-_events_dispatcher_task'))

    async def params_subscribe(
            self,
            class_code: str,
            sec_code: str,
            update_interval_sec: Union[List[float], float],
            params_list: List[str]
    ) -> Dict[str, Any]:
        """
        Requests Quik params subscription for unique combination of (class_code, sec_code) and initializes params cache.

        Notes:
            - Only one subscription per (class_code, sec_code) allowed, extend params_list if you need more fields
            - params_list names casted to lower case!
            - Method can support any instrument type, including options
            - Values of the params are casted to float, str, datetime.datetime,
            datetime.time (depending on type returned by Quik Lua API)
            - Missing or currently unfilled values may be None (for objects) or float('nan') for numeric
            - The client will take care of data updates under the hood,
            just call self.params_get(class_code: str, sec_code: str) to get most recent data

        :param class_code: Quik class code
        :param sec_code: Quik sec code
        :param update_interval_sec: poll interval for parameter,
        or maybe a list of number per each element in `params_list`
        :param params_list: a list of parameter to track (case insensitive, but casted to lower case later!),
        e.g. ['STATUS', 'lotsize', 'BID', 'offer']
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

        :raises QuikLuaException: if param name is not valid or not applicable to instrument
        (for example, volatility for future contract)

        :return: Nothing
        """
        if self._is_shutting_down:
            raise asyncio.CancelledError()

        if not isinstance(update_interval_sec, (list, tuple, float, int)):
            raise QuikLuaException(f'{(class_code, sec_code)} update_interval_sec must be list, tuple, or float/int')

        if isinstance(update_interval_sec, (list, tuple)):
            if len(update_interval_sec) != len(params_list):
                raise QuikLuaException(
                    f'{(class_code, sec_code)} update_interval_sec list must have equal length with params_list')
        else:
            if update_interval_sec <= 0:
                raise QuikLuaException(f'{(class_code, sec_code)} update_interval_sec must be positive')

        # self.get_lock(): Do not allow creation of more than self.n_simultaneous_sockets
        params_to_watch = []
        try:
            if self.verbosity > 1:
                self.log.debug(f'params_subscribe({class_code}, {sec_code}, {params_list})')

            # Request params
            for param in params_list:
                await self.zmq_pool_data.rpc_call('ParamRequest', class_code=class_code, sec_code=sec_code,
                                                  db_name=param)

            # Fill initial values of the params
            cache = ParamCache(class_code, sec_code, params_list)
            for i, param in enumerate(params_list):
                param_ex_api_response = await self.zmq_pool_data.rpc_call('getParamEx2', class_code=class_code,
                                                                          sec_code=sec_code, param_name=param)
                cache.process_param(param, param_ex_api_response)
                if isinstance(update_interval_sec, (list, tuple)):
                    assert isinstance(update_interval_sec[i], (float, int, float, int)), \
                        f'update_interval_sec: Expected float got {update_interval_sec[i]}'
                    params_to_watch.append((class_code, sec_code, param, update_interval_sec[i]))
                else:
                    params_to_watch.append((class_code, sec_code, param, float(update_interval_sec)))

            # Add new params to watcher
            async with self._params_watcher.lock:
                if params_to_watch:
                    self._params_watcher.subscribed(params_to_watch)  # type: ignore
                self._params_cache[(class_code, sec_code)] = cache

            if self.verbosity > 1:
                self.log.debug('params_subscribe(%s, %s) -- initial values -- %s',
                               class_code, sec_code, cache.params)
        except zmq.ZMQError as exc:
            raise QuikLuaConnectionException(repr(exc)) from exc
        finally:
            pass

        return self._params_cache[(class_code, sec_code)].params

    async def params_unsubscribe(self, class_code: str, sec_code: str) -> None:
        """
        Unsubscribe all params for (class_code, sec_code)
        :param class_code:
        :param sec_code:
        :return:
        """

        if (class_code, sec_code) not in self._params_cache:
            return

        cache = self._params_cache[(class_code, sec_code)]
        params_to_unwatch = [(class_code, sec_code, param) for param in cache.params.keys()]

        if params_to_unwatch:
            async with self._params_watcher.lock:
                self._params_watcher.unsubscribed(params_to_unwatch)  # type: ignore

                if self.verbosity > 1:
                    self.log.debug(f'params_unsubscribe({class_code}, {sec_code})')

                # Request params
                for param in cache.params.keys():
                    await self.zmq_pool_data.rpc_call('CancelParamRequest', class_code=class_code, sec_code=sec_code,
                                                      db_name=param)

                del self._params_cache[(class_code, sec_code)]

    async def _params_watch_task(self) -> None:
        """
        AsyncIO task for updating price parameters
        :return:
        """

        while True:

            await asyncio.sleep(self._params_poll_interval)
            b_time = time.time()

            if self._is_shutting_down:
                # Close task
                raise asyncio.CancelledError()

            try:
                async with self._params_watcher.lock:
                    candidates = self._params_watcher.get_update_candidates()
                    if len(candidates) > 0:

                        for (class_code, sec_code, param) in candidates['key']:
                            cache = self._params_cache.get((class_code, sec_code), None)
                            if cache is None:
                                # In rare situation, this cache may be missing
                                continue
                            rpc_result = await self.zmq_pool_data.rpc_call('getParamEx2', class_code=class_code,
                                                                           sec_code=sec_code, param_name=param)
                            cache.process_param(param, rpc_result)

                            if cache.last_quote_change_utc is not None:
                                if self._last_quote_processed is None:
                                    self._last_quote_processed = cache.last_quote_change_utc
                                else:
                                    self._last_quote_processed = max(self._last_quote_processed,
                                                                     cache.last_quote_change_utc)

                        self._params_watcher.set_candidate_updates(candidates)
                        if self.verbosity > 2:
                            self.log.debug('#%s instruments params updated in %ssec',
                                           len(candidates), time.time() - b_time)
            except QuikLuaConnectionException as exc:
                # Typical socket is not available
                if self.verbosity > 1:
                    self.log.error('_events_watch_task() -- Socket error: %s', repr(exc))
                # Wait for a new connection trial
                await asyncio.sleep(10)
            except Exception as exc:
                self.log.error('_params_watch_task() -- %s', repr(exc))

    def params_get(self, class_code: str, sec_code: str) -> Dict[str, Any]:
        """
        Get (class_code, sec_code) that tracked by self.params_subscribe(...)

        :param class_code:
        :param sec_code:
        :raises QuikLuaException: if (class_code, sec_code) is not subscribed
        :return: a dictionary with all params
            Example:
            > await qclient.params_subscribe('SPBFUT', 'RIH1',
            ['bid', 'offer', 'last', 'MAT_DATE', 'time', 'STEPPRICET'])
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
        if self.last_quote_processed_utc is not None:
            if (dtm.datetime.utcnow() - self.last_quote_processed_utc).total_seconds() > self._params_delay_timeout_sec:
                raise QuikLuaConnectionException('Suspected quotes processing delays')
        return params.params

    async def main(self) -> None:
        """
        Main client entry point, also does some low level initialization of the client app.

        Consider to override this method in child class and call `await super().main()` in it

        :return:
        """
        if self._is_shutting_down:
            raise asyncio.CancelledError()

        # Send a heartbeat to test the RPC connection!
        await self.heartbeat()

    async def shutdown(self) -> None:
        """
        Shutdowns Quik Lua Socket connection and unsubscribes the data

        You MUST call this method and the application shutdown to prevent memory leaks of the Quik terminal

        :return: Nothing
        """
        if self._is_shutting_down:
            # Avoid duplicate calls
            return

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
                if cache.ds_uuid and self.verbosity > 1:
                    self.log.debug('datasource.Close: %s %s', key, cache.ds_uuid)
                await self.rpc_call("datasource.Close", datasource_uuid=cache.ds_uuid)

        if self._params_cache:
            if self.verbosity > 0:
                self.log.debug('Cleanup quik param cache')

            all_subscriptions = list(self._params_cache.keys())
            for (class_code, sec_code) in all_subscriptions:
                await self.params_unsubscribe(class_code, sec_code)

        self.zmq_pool_data.close()
        self.zmq_pool_rpc.close()
        self.zmq_context.destroy()

    async def heartbeat(self) -> Any:
        """
        Checks LUA socket connection and last Quik record time

        Also checks health status of all client background tasks like params update routines.

        :return: LUA Func getInfoParam('LASTRECORDTIME') or raises QuikLuaConnectionException() if socket disconnected
        """
        if self._is_shutting_down:
            raise asyncio.CancelledError()
        if self.verbosity > 1:
            self.log.debug('Heartbeat call sent')

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

        result = await self.rpc_call('getInfoParam', param_name='LASTRECORDTIME')
        if result and 'info_param' in result and result['info_param']:
            last_rec_dt = dtm.datetime.combine(
                dtm.datetime.utcnow().date(),
                dtm.time(*[int(t) for t in result['info_param'].split(':')])  # type: ignore
            )
            self._last_data_processed = pd.Timestamp(last_rec_dt, tz='Europe/Moscow').tz_convert('UTC').replace(
                tzinfo=None)
        else:
            self._last_data_processed = None

        return result

    async def clear_price_history_cache(self, class_code: str, sec_code: str, interval: str) -> None:
        cache_key = (class_code, sec_code, interval)

        if cache_key in self._quote_cache:
            try:
                cache = self._quote_cache[cache_key]
                if cache.ds_uuid:
                    await self.rpc_call("datasource.Close", datasource_uuid=cache.ds_uuid)
            finally:
                del self._quote_cache[cache_key]

    async def get_price_history(
            self,
            class_code: str,
            sec_code: str,
            interval: str,
            use_caching: bool = True,
            copy: bool = True,
            date_from: dtm.datetime = dtm.datetime(1900, 1, 1)
    ) -> pd.DataFrame:
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
        :param date_from: date from
        :exception: QuikLuaNoHistoryException - if no history found or backfill is too long
        (see self.history_backfill_interval_sec param)
        :return: pd.DataFrame(['o', 'h', 'l', 'c', 'v']) with DateTime Index (TZ in Moscow time, but tz-naive)
        """

        if self._is_shutting_down:
            raise asyncio.CancelledError()

        if use_caching:
            cache_key = (class_code, sec_code, interval)
            if cache_key in self._quote_cache:
                cache = self._quote_cache[cache_key]
            else:
                cache = HistoryCache(class_code, sec_code, interval,
                                     cache_update_min_interval_sec=self._quote_cache_min_update_sec)
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
                    return cache.data.copy()  # type: ignore
                return cache.data  # type: ignore

            if cache.ds_uuid is None:
                # Cache is not initialized, create a new DataSource in Quik
                response = await self.zmq_pool_data.rpc_call("datasource.CreateDataSource",
                                                             **{"class_code": class_code, "sec_code": sec_code,
                                                                "interval": interval, "param": ""})

                ds_uuid = response['datasource_uuid']
                cache.ds_uuid = ds_uuid
                if self.verbosity > 0:
                    self.log.debug('Created DataSource: %s uuid: %s', (class_code, sec_code, interval), ds_uuid)
            else:
                # re-using Quik uuid from datasource.CreateDataSource with the same (class_code, sec_code, interval)
                # combination
                ds_uuid = cache.ds_uuid

            n_tries = 0
            time_begin = time.time()
            response_size = {'value': 0}
            while response_size['value'] == 0:
                if self._is_shutting_down:
                    raise asyncio.CancelledError()

                #
                # It will take some time to load initial data from Quik server, and initially
                # it returns zero side for some period of time,
                #       just wait until the requested data becomes available, or maybe sec_code
                #       is incorrect then raise an exception
                if time.time() - time_begin > self.history_backfill_interval_sec:
                    raise QuikLuaNoHistoryException(
                        f'No history returned, backfill timeout >'
                        f' {self.history_backfill_interval_sec}sec and #{n_tries} tries')
                response_size = await self.zmq_pool_data.rpc_call("datasource.Size",
                                                                  datasource_uuid=ds_uuid)
                # Wait until quik backfills the data from the server, may take several seconds!
                if response_size['value'] == 0:
                    # print('wait')
                    await asyncio.sleep(0.2)
                else:
                    break
                n_tries += 1
            bar_count = response_size['value']
            if self.verbosity > 0:
                self.log.debug('Got the initial data after: %s',
                               f'{time.time() - time_begin:0.2f}sec ({(class_code, sec_code, interval)})')

            result = []
            time_begin = time.time()
            if cache.last_bar_date:
                # Cache was populated
                last_bar_date = cache.last_bar_date
            else:
                last_bar_date = date_from

            for i in range(bar_count, 0, -1):
                if self._is_shutting_down:
                    raise asyncio.CancelledError()
                _dt = (await self.zmq_pool_data.rpc_call("datasource.T", datasource_uuid=ds_uuid, candle_index=i))[
                    'time']
                bar_date = dtm.datetime(_dt['year'], _dt['month'], _dt['day'], _dt['hour'], _dt['min'], _dt['sec'],
                                        _dt['ms'] * 1000)

                if bar_date < last_bar_date:
                    # Update only until last bar in cache (including it, to update the most recent bar)
                    break

                candle_open = \
                    (await self.zmq_pool_data.rpc_call("datasource.O", datasource_uuid=ds_uuid, candle_index=i))[
                        'value']
                candle_high = \
                    (await self.zmq_pool_data.rpc_call("datasource.H", datasource_uuid=ds_uuid, candle_index=i))[
                        'value']
                candle_low = \
                    (await self.zmq_pool_data.rpc_call("datasource.L", datasource_uuid=ds_uuid, candle_index=i))[
                        'value']
                candle_close = \
                    (await self.zmq_pool_data.rpc_call("datasource.C", datasource_uuid=ds_uuid, candle_index=i))[
                        'value']
                candle_vol = \
                    (await self.zmq_pool_data.rpc_call("datasource.V", datasource_uuid=ds_uuid, candle_index=i))[
                        'value']

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
                await self.zmq_pool_data.rpc_call("datasource.Close", datasource_uuid=ds_uuid)

            # Update history cache if applicable
            if result:
                quotes_df = pd.DataFrame(result).set_index('dt').sort_index()
            else:
                return pd.DataFrame()
            if self.verbosity > 1:
                self.log.debug('%s %s bars updated in cache', sec_code, len(quotes_df))
            cache.process_history(quotes_df)

            if self.verbosity > 2:
                self.log.debug(
                    'Historical quotes processed in %s',
                    f'{time.time() - time_begin:0.2f}sec ({(class_code, sec_code, interval)})'
                )

            # Return full history from cache
            if copy:
                return cache.data.copy()  # type: ignore
            return cache.data  # type: ignore

    async def rpc_call(self, rpc_func: str, **rpc_args: Any) -> Any:
        """
        Sends RPC call to the Quick Lua endpoint

        Examples:
            await self.rpc_call('getScriptPath') -> returns {'script_path': 'C:\\QUIK\\lua\\quik-lua-rpc'}
            await self.rpc_call('message', message='Hello world', icon_type='WARNING') -> returns{'result': 1}
            await self.rpc_call('NotExistingLuaFunc') -> raises QuikLuaException:
            Code: 404 -- QLua-функция с именем 'NotExistingLuaFunc' не найдена.
        :param rpc_func: RPC function name
        :param rpc_args: any arbitrary set of arguments that passed to RPC function
        :raises:
            QuikLuaException - on function errors
            QuikLuaConnectionException - on ZeroMQ connectivity errors
        :return: dict with result
        """
        return await self.zmq_pool_rpc.rpc_call(rpc_func, **rpc_args)  # flake8: noqa: C901

    #
    #
    #   Events processing
    #
    #
    async def _events_watch_task(self) -> None:
        """
        AsyncIO task for reading event socked and queuing event handlers

        :return:
        """
        if self._event_host is None:
            # Event host is disabled, just skipping
            self.log.debug(f'Watching events: {self._event_host}')
            return

        if self.verbosity > 0:
            self.log.debug('Watching events: %s', self._event_host)

        _socket = None

        while True:

            try:
                json_data = None
                event_header = None
                async with self._lock_event:
                    if _socket is None:
                        self.log.debug('Connecting socket: SUB %s', self._event_host)
                        _socket = self.zmq_context.socket(zmq.SUB)
                        # _socket.setsockopt(zmq.RCVTIMEO, 100)
                        # Make raising exceptions when receiving socket timeout or not exists
                        _socket.setsockopt(zmq.LINGER, 1000)  # Free socket at socket.close timeout
                        _socket.connect(self._event_host)
                        _socket.setsockopt(zmq.SUBSCRIBE, b"")

                    if self._is_shutting_down:
                        # Close task
                        raise asyncio.CancelledError()

                    response = await _socket.recv()

                    if b'On' in response:  # type: ignore
                        # New event header
                        event_header = response.decode()  # type: ignore
                        if event_header in ['OnDisconnected', 'OnStop', 'OnClose']:
                            raise zmq.ZMQError(msg='Quik disconnected or stopped')

                        json_data = await _socket.recv_json()  # type: ignore

                        if self._event_filter is None or event_header.lower() in self._event_filter:
                            if self.verbosity > 2:
                                self.log.debug('%s: %s', event_header, json_data)

                            self._event_que.put_nowait((event_header, dtm.datetime.now(), json_data))
            except asyncio.CancelledError as e:
                raise e
            except Exception as exc:
                self._last_event_processed = None
                if _socket:
                    try:
                        _socket.setsockopt(zmq.LINGER, 0)
                        _socket.close()
                    except Exception as e:
                        self.log.error('_events_watch_task() -- error closing socket: %s', e)
                _socket = None

                if isinstance(exc, zmq.ZMQError):
                    if self.verbosity > 1:
                        self.log.error('_events_watch_task() -- Socket error: %s', repr(exc))
                else:
                    self.log.exception('_events_watch_task() exception: %s: data %s', event_header, json_data)

                # Wait for a new connection trial
                await asyncio.sleep(1)

    async def _events_dispatcher_task(self) -> None:
        """
        AsyncIO task for reading event socked and queuing event handlers

        :return:
        """
        if self._event_host is None:
            # Event host is disabled, just skipping
            return None

        while True:
            if self._is_shutting_down:
                # Close task
                raise asyncio.CancelledError()

            try:
                e_header, e_dt, e_data = await self._event_que.get()

                if (dtm.datetime.now() - e_dt).total_seconds() > 30:
                    self.log.error('Event processing delays > 30 sec, check client code performance')

                self._last_event_processed = dtm.datetime.utcnow()

                await self.on_new_event(e_header, e_data)
            except asyncio.CancelledError as e:
                raise e
            except Exception as e:
                self.log.exception('Exception: %s', e)

    async def on_new_event(self, event_name: str, event_data: dict) -> None:
        """
        Processing event data

        :param event_name: event name as in Quik Lua
        :param event_data:

        :return:
        """
        if self._event_callback_coro is not None:
            await self._event_callback_coro(event_name, event_data)

    @property
    def last_data_processed_utc(self) -> Optional[dtm.datetime]:
        return self._last_data_processed

    @property
    def last_quote_processed_utc(self) -> Optional[dtm.datetime]:
        return self._last_quote_processed

    @property
    def last_event_processed_utc(self) -> Optional[dtm.datetime]:
        return self._last_event_processed

    def stats(self) -> Dict[str, Any]:

        rpc_stats = self.zmq_pool_rpc.stats()
        data_stats = self.zmq_pool_data.stats()

        return {
            'zmq_rpc_stats': rpc_stats,
            'zmq_data_stats': data_stats,
            'params_instruments': len(self._params_cache),
            'params_total_count': self._params_watcher.count(),
            'history_cache_count': len(self._quote_cache),
            'last_data_processed_utc': self._last_data_processed,
            'last_quote_processed_utc': self._last_quote_processed,
            'last_event_processed_utc': self._last_event_processed,
        }

    def stats_reset(self) -> None:
        self.zmq_pool_rpc.stats_reset()
        self.zmq_pool_data.stats_reset()
