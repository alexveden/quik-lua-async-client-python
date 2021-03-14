import asyncio
from aioquiklua import QuikLuaClientBase, QuikLuaException, QuikLuaConnectionException, QuikLuaNoHistoryException
import traceback
import time


async def main():
    """
    Можно использовать QuikLuaClientBase без определения дочернего класса
    """

    qclient = QuikLuaClientBase("tcp://localhost:5560",  # RPC сокет
                                None,  # PUB сокет
                                socket_timeout=100,  # Таймаут сокета после которого он выдает ошибку (в миллисекундах)
                                n_simultaneous_sockets=5,  # Количество одновременно открытых сокетов
                                history_backfill_interval_sec=10,  # Таймаут на ожидание истории (в секундах) (обычно занимает менее 1 сек)
                                cache_min_update_sec=0.2,  # Время актуальности истории котировок к кеше, после последнего обновления
                                verbosity=2,  # Включаем  debugging information (чем выше значение тем больше идет в лог)
                                # logger=logging.getLogger('testlog') # Можно задать кастомный логгер
                               )

    try:
        # Вызываем initialize() основного класса для инициализации внутренних переменных
        await qclient.initialize()

        # Тут вызываем логику модели, подписываемся на события и т.п.
        class_list = await qclient.rpc_call('getClassesList')
        print('RPC: getClassesList')
        print(class_list)

        print(f'Press ctrl+c to stop')
        while True:
            # Нажмите ctrl+c чтобы завершить
            # Получаем heartbeat(), он возвращает результат RPC getInfoParam('LASTRECORDTIME')
            await qclient.heartbeat()
            await asyncio.sleep(30)
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
