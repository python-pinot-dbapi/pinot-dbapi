from pinotdb import connect

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy.orm import sessionmaker


def run_pinot_live_example() -> None:
    # Query pinot.live with pinotdb connect
    conn = connect(host="pinot-broker.pinot.live", port=443, path="/query/sql", scheme="https",
                   extra_request_headers="Database=default")
    curs = conn.cursor()
    sql = "SELECT * FROM airlineStats LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    curs.execute(sql)
    for row in curs:
        print(row)

    # Query pinot.live with sqlalchemy
    engine = create_engine(
        "pinot+https://pinot-broker.pinot.live:443/query/sql?controller=https://pinot-controller.pinot.live/",
        connect_args={"query_options": "timeoutMs=10000"}
    )  # uses HTTP by default :(

    metadata = MetaData()
    airlineStats = Table(
        "airlineStats",
        metadata,
        autoload_with=engine,
        schema="default",
    )
    print(f"\nSending Count(*) SQL to Pinot")
    query = select(func.count()).select_from(airlineStats)
    with engine.connect() as connection:
        print(connection.execute(query).scalar())

    Session = sessionmaker(bind=engine)
    session = Session()
    query = (
        select(
            column("AirlineID"),
            func.max(column("AirTime")).label("max_airtime"),
        )
        .select_from(airlineStats)
        .group_by(column("AirlineID"))
        .order_by(text("max_airtime DESC"))
        .limit(10)
    )
    print(
        '\nSending SQL: "SELECT AirlineID, max(AirTime) AS max_airtime FROM airlineStats '
        'GROUP BY AirlineID ORDER BY max_airtime DESC LIMIT 10" to Pinot'
    )
    with engine.connect() as connection:
        print(connection.execute(query).fetchall())


def run_main():
    run_pinot_live_example()


if __name__ == '__main__':
    run_main()
