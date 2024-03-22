from pinotdb import connect
import time
import pytest
import httpx

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 \
##    -d apachepinot/pinot:latest QuickStart -type hybrid

def run_pinot_quickstart_timeout_example() -> None:

    #Test 1 : Try without timeout. The request should succeed.

    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http",
                   extra_request_headers="Database=default")
    curs = conn.cursor()
    sql = "SELECT * FROM airlineStats LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    curs.execute(sql)
    conn.close()

    #Test 2 : Try with timeout=None. The request should succeed.

    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http", timeout=None,
                   extra_request_headers="Database=default")
    curs = conn.cursor()
    sql = "SELECT count(*) FROM airlineStats LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    curs.execute(sql)
    conn.close()

    #Test 3 : Try with a really small timeout. The query should raise an exception.

    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http", timeout=0.001,
                   extra_request_headers="Database=default")
    curs = conn.cursor()
    sql = "SELECT AirlineID, sum(Cancelled) FROM airlineStats WHERE Year > 2010 GROUP BY AirlineID LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    with pytest.raises(httpx.ReadTimeout):
        curs.execute(sql)
    conn.close()


def run_main():
    run_pinot_quickstart_timeout_example()


if __name__ == '__main__':
    run_main()
