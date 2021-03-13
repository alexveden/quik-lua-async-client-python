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
    qclient = QuikLuaClientSample("tcp://localhost:5560", None)
    asyncio.run(qclient.main())
