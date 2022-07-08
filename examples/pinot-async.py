import asyncio

import httpx
from pinotdb import connect


async def foo():
    async with connect(host='localhost', port=8099, path='/query/sql',
                       scheme='https', verify_ssl=False, use_async=True) as conn:
        curs = await conn.execute_async("""
            SELECT count(*)
              FROM foo
             LIMIT 10
        """)
        for row in curs:
            print(row)

    conn = connect(
        host='localhost', port=8099, path='/query/sql', scheme='https',
        verify_ssl=False, use_async=True)

    curs = conn.cursor()
    await curs.execute_async("""
        SELECT count(*)
          FROM foo
         LIMIT 10
    """)
    for row in curs:
        print(row)

    # Externally managed client session
    session = httpx.AsyncClient(verify=False)
    conn = connect(
        host='localhost', port=8099, path='/query/sql', scheme='https',
        verify_ssl=False, use_async=True, session=session)

    await curs.execute_async("""
        SELECT count(*)
          FROM foo
         LIMIT 10
    """)
    for row in curs:
        print(row)

    # close all cursors
    await conn.close_async()

    # don't forget to close the session in the case where it was provided to
    # connect
    await session.aclose()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(foo())
