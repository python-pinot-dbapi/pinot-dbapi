from pinotdb import connect

from sqlalchemy import *
from sqlalchemy.dialects import registry
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy.orm import sessionmaker

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 \
##    -d apachepinot/pinot:latest QuickStart -type MULTI_STAGE

def run_pinot_quickstart_multi_stage_example() -> None:
    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http", use_multistage_engine=True,
                   extra_request_headers="Database=default")
    curs = conn.cursor()

    sql = "SELECT a.playerID, a.runs, a.yearID, b.runs, b.yearID FROM baseballStats_OFFLINE AS a JOIN baseballStats_OFFLINE AS b ON a.playerID = b.playerID WHERE a.runs > 160 AND b.runs < 2 LIMIT 10"
    print(f"\nSending SQL to Pinot: {sql}")
    curs.execute(sql)
    for row in curs:
        print(row)

    sql = "SELECT a.playerName, a.teamID, b.teamName FROM baseballStats_OFFLINE AS a JOIN dimBaseballTeams_OFFLINE AS b ON a.teamID = b.teamID LIMIT 10";
    print(f"\nSending SQL to Pinot: {sql}")
    curs.execute(sql)
    for row in curs:
        print(row)


def run_pinot_quickstart_multi_stage_sqlalchemy_example() -> None:
    registry.register("pinot", "pinotdb.sqlalchemy", "PinotMultiStageDialect")

    engine = create_engine(
        "pinot://localhost:8000/query/sql?controller=http://localhost:9000/",
        connect_args={"useMultistageEngine": "true"}
    )  # uses HTTP by default :(
    # engine = create_engine('pinot+http://localhost:8000/query/sql?controller=http://localhost:9000/')
    # engine = create_engine('pinot+https://localhost:8000/query/sql?controller=http://localhost:9000/')
    from sqlalchemy import text

    with engine.connect() as connection:
        result = connection.execute(text("SELECT playerID, runs, yearID FROM baseballStats WHERE runs > 160 LIMIT 10"))
        for row in result:
            print(row)
        result = connection.execute(text("SELECT a.playerID, a.runs, a.yearID, b.runs, b.yearID FROM baseballStats AS a JOIN baseballStats AS b ON a.playerID = b.playerID WHERE a.runs > 160 AND b.runs < 2 LIMIT 10"))
        for row in result:
            print(row)

def run_pinot_quickstart_multi_stage_sqlalchemy_example_2() -> None:

    # Multi-stage engine requires /query/sql endpoint.
    engine = create_engine(
        "pinot://localhost:8000/query/sql?controller=http://localhost:9000/",
        connect_args={"useMultistageEngine": "true"}
    )  # uses HTTP by default :(
    # engine = create_engine('pinot+http://localhost:8000/query/sql?controller=http://localhost:9000/')
    # engine = create_engine('pinot+https://localhost:8000/query/sql?controller=http://localhost:9000/')
    from sqlalchemy import text

    with engine.connect() as connection:
        result = connection.execute(text("SELECT playerID, runs, yearID FROM baseballStats WHERE runs > 160 LIMIT 10"))
        for row in result:
            print(row)
        result = connection.execute(text("SELECT a.playerID, a.runs, a.yearID, b.runs, b.yearID FROM baseballStats AS a JOIN baseballStats AS b ON a.playerID = b.playerID WHERE a.runs > 160 AND b.runs < 2 LIMIT 10"))
        for row in result:
            print(row)


def run_main():
    run_pinot_quickstart_multi_stage_example()
    run_pinot_quickstart_multi_stage_sqlalchemy_example()
    run_pinot_quickstart_multi_stage_sqlalchemy_example_2()


if __name__ == "__main__":
    run_main()
