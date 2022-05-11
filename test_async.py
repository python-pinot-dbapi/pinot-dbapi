import asyncio
from pinotdb import connect

conn = connect(host='localhost', port=44444, path='/query/sql',
               scheme='https', verify_ssl=False)


async def foo():
    curs = conn.cursor()
    await curs.execute("""
        SELECT count(*)
          FROM calls
         LIMIT 10
    """)
    for row in curs:
        print(row)
    await conn.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(foo())
