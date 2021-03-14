import asyncio
from functools import wraps
import time
from aioquiklua import QuikLuaClientBase, QuikLuaException, QuikLuaConnectionException, QuikLuaNoHistoryException


def timing(f):
    @wraps(f)
    async def wrap(*args, **kw):
        ts = time.time()
        result = await f(*args, **kw)
        te = time.time()
        print('func:%r args:[%r, %r] took: %2.4f sec' % (f.__name__, args, kw, te - ts))
        return result

    return wrap


class QuikLuaClientBenchmarker(QuikLuaClientBase):
    @timing
    async def test_heartbeat_sync(self, n_steps):
        for i in range(n_steps):
            await self.heartbeat()

    @timing
    async def test_heartbeat_async(self, n_steps):
        await asyncio.gather(*[self.heartbeat() for i in range(n_steps)])
    @timing
    async def test_getClassesList_sync(self, n_steps):
        for i in range(n_steps):
            await self.rpc_call('getClassesList')

    @timing
    async def test_getClassesList_async(self, n_steps):
        await asyncio.gather(*[self.rpc_call('getClassesList') for i in range(n_steps)])

    @timing
    async def test_historical_quotes_cached_async(self, n_steps):
        await asyncio.gather(*[self.get_price_history('SPBFUT', 'RIH1', "INTERVAL_M1", use_caching=True) for i in range(n_steps)])

    @timing
    async def test_historical_quotes_cached_before_async(self, n_steps):
        await asyncio.gather(*[self.get_price_history('SPBFUT', 'RIH1', "INTERVAL_M1", use_caching=True) for i in range(n_steps)])

    @timing
    async def test_historical_quotes_no_cache(self, n_steps):
        await asyncio.gather(*[self.get_price_history('SPBFUT', 'RIH1', "INTERVAL_M1", use_caching=False) for i in range(n_steps)])

    async def main(self):
        await super().main()
        n_steps = 1000
        await self.test_heartbeat_sync(n_steps)
        await self.test_heartbeat_async(n_steps)
        await self.test_getClassesList_sync(n_steps)
        await self.test_getClassesList_async(n_steps)
        await self.test_historical_quotes_cached_async(5)

        # Let cache timeout pass
        await asyncio.sleep(1)
        await self.test_historical_quotes_cached_before_async(1)

        await self.test_historical_quotes_no_cache(5)

        # Clean up
        await self.shutdown()


if __name__ == '__main__':
    qclient = QuikLuaClientBenchmarker("tcp://localhost:5560", None)
    asyncio.run(qclient.main())
