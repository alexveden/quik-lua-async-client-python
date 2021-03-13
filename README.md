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
Просто создайте новый класс и запустите его `main()` в `asyncio.run()`

```python
import asyncio
from aioquiklua import QuikLuaClientBase, QuikLuaException, QuikLuaConnectionException, QuikLuaNoHistoryException
import traceback
import time

class QuikLuaClientSample(QuikLuaClientBase):

    async def main(self):
        # Вызываем main() основного класса для инициализации внутренних переменных
        await super().main()

        try:
            # Тут вызываем логику модели, подписываемся на события и т.п.
            class_list = await self.rpc_call('getClassesList')
            print('RPC: getClassesList')
            print(class_list)

            # RPC с параметрами
            rpc_result = await self.rpc_call('message', message='Hello world', icon_type='WARNING')
            print('RPC: message')
            print(rpc_result)

            # Заказываем историю котировок (первый запуск может занимать до 10 секунд), потом котировки заполняют кеш и
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

            while True:
                # Нажмите ctrl+c чтобы завершить
                # Получаем heartbeat(), он возвращает результат RPC getInfoParam('LASTRECORDTIME')
                await self.heartbeat()
                await asyncio.sleep(1)
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
                                  )
    asyncio.run(qclient.main())
```

## Производительность
См. [benchmark.py](https://github.com/alexveden/quik-lua-async-client-python/blob/master/benchmarks.py) в корне репозитория

В целом вот примерно что у меня получается:
1. `await self.heartbeat()` занимает 1.4мс синхронно, и 0.8мс асинхронно на запрос
2. `await self.rpc_call('getClassesList')` занимает 1.4мс синхронно, и 0.8мс асинхронно на запрос
3. Получение истории 1мин таймфрейм, около 10 секунд на инициализацию кэша (парсинг полной истории), потом
всего 32мс, т.к. получает из кэша только последний бар и обновляет его.
4. Если последующий запрос истории пришел ранее чем 200мс (можно поменять в конструкторе класса клиента!) чем последнее обновление, клиент не будет
запрашивать сокет, а вернет, то что у него в памяти.
5. Можно использовать сколько угодно параллельных запросов (напр. 1000 одновременно), клиент сбалансирует нагрузку, однако
по дефолту устанавливается только 5 соединений, я не увидел прироста от большего числа, зато стабильность страдала. 
   
## Документация
Друзья, читайте код! Все функции содержат docstrings, и комменты в коде.

## Стабильность
Я сделал все возможное, чтобы сделать работу кода максимально предсказуемой. Но, как всегда, ожидайте баги
и используйте на свой страх и риск. Сам настроен использовать этого клиента в продакшене.

Версия: ранняя альфа 0.0.0.1, в разработке.

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