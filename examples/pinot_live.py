from pinotdb import connect

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy.orm import sessionmaker


def run_pinot_live_example() -> None:
    # Query pinot.live with pinotdb connect
    conn = connect(host="pinot-broker.pinot.live", port=443, path="/query/sql", scheme="https")
    curs = conn.cursor()
    sql = "SELECT * FROM airlineStats LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    curs.execute(sql)
    for row in curs:
        print(row)

    # Query pinot.live with sqlalchemy
    engine = create_engine(
        "pinot+https://pinot-broker.pinot.live:443/query/sql?controller=https://pinot-controller.pinot.live/"
    )  # uses HTTP by default :(

    airlineStats = Table("airlineStats", MetaData(bind=engine), autoload=True)
    print(f"\nSending Count(*) SQL to Pinot")
    query=select([func.count("*")], from_obj=airlineStats)
    print(engine.execute(query).scalar())

    Session = sessionmaker(bind=engine)
    session = Session()
    query = select(
        [column("AirlineID"), func.max(column("AirTime")).label("max_airtime")],
        from_obj=airlineStats,
        group_by=[column("AirlineID")],
        order_by=text("max_airtime DESC"),
        limit="10",
    )
    print(
        f'\nSending SQL: "SELECT playerName, sum(runs) AS sum_runs FROM "baseballStats"'
        f' WHERE yearID>=2000 GROUP BY playerName ORDER BY sum_runs DESC LIMIT 5" to Pinot'
    )
    print(engine.execute(query).fetchall())


def run_main():
    run_pinot_live_example()


if __name__ == '__main__':
    run_main()
