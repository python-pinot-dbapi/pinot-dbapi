import asyncio
import time

import httpx
from pinotdb import connect_async


async def run_pinot_async_example():
    async with connect_async(host='localhost', port=8000, path='/query/sql',
                             scheme='http', verify_ssl=False) as conn:
        curs = await conn.execute("""
            SELECT count(*)
              FROM baseballStats
             LIMIT 10
        """)
        for row in curs:
            print(row)

    # Externally managed client session can also be passed to connect_async
    session = httpx.AsyncClient(verify=False)
    conn = connect_async(
        host='localhost', port=8000, path='/query/sql', scheme='http',
        verify_ssl=False, session=session)

    # launch 10 requests in parallel spanning a limit/offset range
    reqs = []
    step = 10
    num_requests = 10
    start = time.perf_counter()
    for i in range(num_requests):
        req = conn.execute(f"""
              SELECT *
              FROM baseballStats
              LIMIT 10, {i * step}
        """)
        reqs.append(req)

    for curs in await asyncio.gather(*reqs):
        for row in curs:
            print(row)

    print(f'{num_requests} requests took {time.perf_counter() - start} '
          'seconds')

    # close all cursors
    await conn.close()

    # don't forget to close the session in the case where it was provided to
    # connect
    await session.aclose()


def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_pinot_async_example())


if __name__ == '__main__':
    run_main()
