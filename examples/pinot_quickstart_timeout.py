from pinotdb import connect
import time
import pytest
import httpx

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 \
##    -d apachepinot/pinot:latest QuickStart -type hybrid

def run_pinot_quickstart_timeout_example() -> None:

    #Test 1 : Try without timeout and wait for more than 5 seconds. The query should execute without exceptions.

    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http")
    curs = conn.cursor()
    sql = "SELECT * FROM airlineStats LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    time.sleep(5.001)
    curs.execute(sql)

    #Test 2 : Try with timeout=None and wait for more than 5 seconds. The query should execute without exceptions.

    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http", timeout=None)
    curs = conn.cursor()
    sql = "SELECT * FROM airlineStats LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    time.sleep(5.001)
    curs.execute(sql)

    #Test 3 : Try with a timeout less than the sleep time. The query should raise an exception.

    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http", timeout=0.001)
    curs = conn.cursor()
    sql = "SELECT * FROM airlineStats LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    with pytest.raises(httpx.ReadTimeout):
        time.sleep(0.002)
        curs.execute(sql)


def run_main():
    run_pinot_quickstart_timeout_example()


if __name__ == '__main__':
    run_main()
