import asyncio

import zmq

from aioquiklua import QuikLuaClientBase, QuikLuaException, QuikLuaConnectionException, QuikLuaNoHistoryException
import traceback
import time

#
# Quik Lua config.json
#
"""
{

    "endpoints": [{
        "type": "RPC", 
        "serde_protocol": "json",
        "active": true, 
        "address": {
            "host": "127.0.0.1",
            "port": 5580
        },

        "auth": {
            "mechanism": "NULL",
        }
    },
    {
        "type": "PUB", 
        "serde_protocol": "json",
        "active": true, 
        "address": {
            "host": "127.0.0.1",
            "port": 5581
        },

        "auth": {
            "mechanism": "NULL",
        }
    },
    
    ]
}

"""


async def custom_event_handler(event_name: str, event_data: dict) -> None:
    print('custom_event_handler')
    print(event_name)
    print(event_data)


class QuikLuaClientSample(QuikLuaClientBase):

    async def on_new_event(self, event_name: str, event_data: dict) -> None:
        """
        Processing event data

        :param event_name: event name as in Quik Lua
        :param event_data:

        :return:
        """
        # вызов super() нужен лишь для того чтобы вызвать custom_event_handler
        await super().on_new_event(event_name, event_data)

        print(event_name)
        print(event_data)

    async def main(self):
        # Вызываем initialize() основного класса для инициализации внутренних переменных
        await super().initialize()

        try:
            while True:
                await asyncio.sleep(1)

            pass
        except asyncio.CancelledError:
            # AsyncIO valid stop
            raise
        except KeyboardInterrupt:
            print('KeyboardInterrupt')
        except:
            print(traceback.format_exc())
        finally:
            # Завершаем выполнение (ОБЯЗАТЕЛЬНО вызывайте shutdown() особенно если вы заказывали историю котировок!)
            await self.shutdown()


if __name__ == '__main__':
    qclient = QuikLuaClientSample("tcp://localhost:5580",               # RPC сокет
                                  None,                                 # PUB сокет
                                  socket_timeout=100,                   # Таймаут сокета после которого он выдает ошибку (в миллисекундах)
                                  n_simultaneous_sockets=5,             # Количество одновременно открытых сокетов
                                  history_backfill_interval_sec=10,     # Таймаут на ожидание истории (в секундах) (обычно занимает менее 1 сек)
                                  cache_min_update_sec=0.2,             # Время актуальности истории котировок к кеше, после последнего обновления
                                  verbosity=3,                          # Включаем  debugging information (чем выше значение тем больше идет в лог)
                                  # logger=logging.getLogger('testlog') # Можно задать кастомный логгер
                                  event_host="tcp://localhost:5581",
                                  event_callback_coro=custom_event_handler,
                                  )
    asyncio.run(qclient.main())
