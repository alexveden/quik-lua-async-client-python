# AsyncIO Python клиент для работы с Quik Lua RPC

Для корректной работы вы должны установить и настроить [Quik-Lua-RPC](https://github.com/Enfernuz/quik-lua-rpc)

## Основные возможности
- Клиент создан на основе асинхронных вызовов AsyncIO
- Клиент использует JSON протокол QuikLua (который в данный момент в статусе альфа)
- Клиент поддерживает параллельные запросы через несколько сокетов и организует SocketPool
- Клиент пока работает только с сокетами без авторизации и только localhost (из соображений безопасности)
- Клиент пока **не поддерживает** подписку на события
- Клиент поддерживает получение исторических данных и их кеширование в памяти
- Клиент хранит данные истории котировок в формате Pandas.DataFrame
- Клиент поддерживает проверку наличия соединения через heartbeat и выдает ошибку если socket QuikLua недоступен
- Клиент поддерживает load-balancing т.е. вы в теории можете запустить 100 асинхронных запросов истории котировок
 и клиент распределит нагрузку, чтобы LUA скрипт на той стороне не упал и не сожрал всю память
- У меня вроде даже работает на Linux Debian с квиком под Wine! Если что, то можно подключиться к квику на 
  виртуальной машине.

## Как использовать
Есть 2 варианта:
- Создаем класс-приложение на основе [`QuikLuaClientBase`](https://github.com/alexveden/quik-lua-async-client-python/blob/378929c980da7e4a9177980373ab2cae9fa69628/aioquiklua/client.py#L27) [examples/sample_client.py](https://github.com/alexveden/quik-lua-async-client-python/blob/75bc2aabaafcd3a28e2e4fc630bc6b5d7f8625d6/examples/sample_client.py#L9)
- Создаем асинхронную функцию main(), а в ней объект [`QuikLuaClientBase`](https://github.com/alexveden/quik-lua-async-client-python/blob/378929c980da7e4a9177980373ab2cae9fa69628/aioquiklua/client.py#L27) [examples/sample_asyncio_app.py](https://github.com/alexveden/quik-lua-async-client-python/blob/75bc2aabaafcd3a28e2e4fc630bc6b5d7f8625d6/examples/sample_asyncio_app.py#L7)

Пример sample_client.py
```python
import asyncio
from aioquiklua import QuikLuaClientBase, QuikLuaException, QuikLuaConnectionException, QuikLuaNoHistoryException
import traceback
import time


class QuikLuaClientSample(QuikLuaClientBase):

    async def main(self):
        # Вызываем initialize() основного класса для инициализации внутренних переменных
        await super().initialize()

        try:
            # Тут вызываем логику модели, подписываемся на события и т.п.
            class_list = await self.rpc_call('getClassesList')
            print('RPC: getClassesList')
            print(class_list)

            # RPC с параметрами
            rpc_result = await self.rpc_call('message', message='Hello world', icon_type='WARNING')
            print('RPC: message')
            print(rpc_result)

            # Заказываем историю котировок (первый запуск можен занимать до 10 секунд), потом котировки заполняют кеш и
            # обновляются только последние данные
            time_begin = time.time()
            print(f'RPC: price history')
            quotes_df = await self.get_price_history('SPBFUT', 'RIH1', 'INTERVAL_M1', use_caching=True)
            print(quotes_df.tail(5))
            print(f'Price backfill took {time.time()-time_begin}sec')

            time_begin = time.time()
            print(f'RPC: price history cached')
            quotes_df = await self.get_price_history('SPBFUT', 'RIH1', 'INTERVAL_M1', use_caching=True)
            print(quotes_df.tail(5))
            print(f'Price backfill took {time.time() - time_begin}sec')

            print(f'Press ctrl+c to stop')
            while True:
                # Нажмите ctrl+c чтобы завершить
                # Получаем heartbeat(), он возвращает результат RPC getInfoParam('LASTRECORDTIME')
                await self.heartbeat()
                await asyncio.sleep(10)
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
    qclient = QuikLuaClientSample("tcp://localhost:5560",               # RPC сокет
                                  None,                                 # PUB сокет
                                  socket_timeout=100,                   # Таймаут сокета после которого он выдает ошибку (в миллисекундах)
                                  n_simultaneous_sockets=5,             # Количество одновременно открытых сокетов
                                  history_backfill_interval_sec=10,     # Таймаут на ожидание истории (в секундах) (обычно занимает менее 1 сек)
                                  cache_min_update_sec=0.2,             # Время актуальности истории котировок к кеше, после последнего обновления
                                  verbosity=2,                          # Включаем  debugging information (чем выше значение тем больше идет в лог)
                                  # logger=logging.getLogger('testlog') # Можно задать кастомный логгер
                                  )
    asyncio.run(qclient.main())

```

## Производительность
См. [examples/benchmark.py](https://github.com/alexveden/quik-lua-async-client-python/blob/75bc2aabaafcd3a28e2e4fc630bc6b5d7f8625d6/examples/benchmarks.py#L49)

В целом вот примерно что у меня получается:
1. `await self.heartbeat()` занимает 1.4мс синхронно, и 0.8мс асинхронно на запрос
2. `await self.rpc_call('getClassesList')` занимает 1.4мс синхронно, и 0.8мс асинхронно на запрос
3. Получение истории 1мин таймфрейм, около 10 секунд на инициализацию кэша (парсинг полной истории). Потом
всего 32мс, т.к. получает из кэша только последние бары с момента предыдущего запроса и обновляет их.
4. Если последующий запрос истории пришел ранее чем 200мс (можно поменять в конструкторе класса клиента!) чем последнее обновление, клиент не будет
запрашивать сокет, а вернет, то что у него в памяти.
5. Можно использовать сколько угодно параллельных запросов (напр. 1000 одновременно), клиент сбалансирует нагрузку, однако
по дефолту устанавливается только 5 соединений, я не увидел прироста от большего числа, зато стабильность страдала. 
   
## Документация
Друзья, читайте код! Все функции содержат docstrings, и комменты в коде.

## Стабильность
Я сделал все возможное, чтобы сделать работу кода максимально предсказуемой. Но, как всегда, ожидайте баги
и используйте на свой страх и риск. 

Версия: ранняя альфа 0.0.0.2, в разработке.

### Quik Lua Config
Вот как выглядит файл конфига `.../QUIK/lua/quik-lua-rpc/config.json`
```json
  {
    "endpoints": [{
        "type": "RPC",
        "serde_protocol": "json",
        "active": true, 
        "address": {
            "host": "127.0.0.1",
            "port": 5560
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
            "port": 5561
        },
        "auth": {
            "mechanism": "NULL", 
        }
    }]
}
```