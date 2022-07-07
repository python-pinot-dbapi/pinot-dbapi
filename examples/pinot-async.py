import asyncio
from pinotdb import connect


async def foo():
    async with connect(host='localhost', port=33333, path='/query/sql',
                       scheme='https', verify_ssl=False) as conn:
        curs = await conn.execute_async("""
            SELECT count(*)
              FROM foo
             LIMIT 10
        """)
        for row in curs:
            print(row)

    conn = connect(
        host='localhost', port=33333, path='/query/sql', scheme='https',
        verify_ssl=False)  # could include use_async here

    curs = conn.cursor(use_async=True)
    await curs.execute_async("""
        SELECT count(*)
          FROM foo
         LIMIT 10
    """)
    for row in curs:
        print(row)
    await conn.close_async()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(foo())
