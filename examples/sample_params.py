import asyncio
from aioquiklua import QuikLuaClientBase, QuikLuaException, QuikLuaConnectionException, QuikLuaNoHistoryException
import traceback
import time


async def main():
    """
    Пример кода для получения параметров
    """

    qclient = QuikLuaClientBase("tcp://localhost:5560",  # RPC сокет
                                None,  # PUB сокет
                                socket_timeout=100,  # Таймаут сокета после которого он выдает ошибку (в миллисекундах)
                                n_simultaneous_sockets=5,  # Количество одновременно открытых сокетов
                                history_backfill_interval_sec=10,  # Таймаут на ожидание истории (в секундах) (обычно занимает менее 1 сек)
                                cache_min_update_sec=0.2,  # Время актуальности истории котировок к кеше, после последнего обновления
                                verbosity=3,  # Включаем  debugging information (чем выше значение тем больше идет в лог)
                                # logger=logging.getLogger('testlog') # Можно задать кастомный логгер
                               )

    try:
        # Вызываем initialize() основного класса для инициализации внутренних переменных
        await qclient.initialize()

        # Просто проверяем коннект
        await qclient.heartbeat()

        #
        # Ключи параметров могут быть в любом регистре, но потом все ключи параметров конвертируются в lower case
        #
        await qclient.params_subscribe('SPBFUT',
                                       'RIH1',
                                       [0.5, 0.5, 0.5, 10, 10, 10],   # Можно задать интервал обновления отдельно для каждого параметра в сек
                                       ['bid', 'offer', 'last', 'MAT_DATE', 'time', 'STEPPRICET'])
        await qclient.params_subscribe('SPBOPT', 'Si40000BC1',
                                       5, # Все параметры будут обновляться раз в 5 сек.
                                       ['bid', 'offer', 'last', 'MAT_DATE', 'time', 'VOLATILITY', 'THEORPRICE', 'STEPPRICET'])

        try:
            # Дважды подписываться на одну пару (class_code, sec_code) нельзя
            await qclient.params_subscribe('SPBFUT', 'RIH1', 1, ['bid', 'offer', 'last', 'MAT_DATE', 'time', 'STEPPRICET'])
        except QuikLuaException as exc:
            print(f'Дважды подписываться на одну пару (class_code, sec_code) нельзя: {repr(exc)}')

        try:
            # Запрос неизвестных параметров ведет к exception
            await qclient.params_subscribe('SPBFUT', 'SiH1', 1, ['abracadabra'])
        except QuikLuaException as exc:
            print(f'Unknown param_name: {repr(exc)}')

        try:
            # Также поля не свойственные типу инструмента вызывают exception
            await qclient.params_subscribe('SPBFUT', 'RIM1', 1, ['VOLATILITY'])
        except QuikLuaException as exc:
            print(f'Volatility is not supported by futures! -- {repr(exc)}')


        print(f'Press ctrl+c to stop')
        while True:
            # Нажмите ctrl+c чтобы завершить
            #
            # В течение рабочего времени торговой сессии значения параметров должны обновляться
            #

            params = qclient.params_get('SPBFUT', 'RIH1')
            # Обратите внимание все ключи параметров конвертируются в lower case
            print(f"Params 'SPBFUT', 'RIH1': {params}")
            params = qclient.params_get('SPBOPT', 'Si40000BC1')
            # Обратите внимание все ключи параметров конвертируются в lower case
            print(f"Params 'SPBOPT', 'Si40000BC1': {params}")
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        # AsyncIO valid stop
        raise
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
    except:
        print(traceback.format_exc())
    finally:
        # Завершаем выполнение (ОБЯЗАТЕЛЬНО вызывайте shutdown() особенно если вы заказывали историю котировок!)
        await qclient.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
