from pinotdb import connect

from sqlalchemy import *
from sqlalchemy.dialects import registry
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy.orm import sessionmaker

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 \
##    -d apachepinot/pinot:latest QuickStart -type batch

def run_quickstart_json_batch_example() -> None:
    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http")
    curs = conn.cursor()
    sql = "SELECT * FROM githubEvents LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    curs.execute(sql)
    for row in curs:
        print(row)

    sql = "SELECT created_at_timestamp FROM githubEvents LIMIT 5"
    print(f"Sending SQL to Pinot: {sql}")
    curs.execute(sql)
    for row in curs:
        print(row)

    sql = "select json_extract_scalar(repo, '$.name', 'STRING'), count(*) from githubEvents" \
          " where json_match(actor, '\"$.login\"=''LombiqBot''') group by 1 order by 2 desc limit 10"
    print(f"\nSending SQL to Pinot: {sql}")
    curs.execute(sql)
    for row in curs:
        print(row)

def run_quickstart_json_batch_sqlalchemy_example() -> None:
    registry.register("pinot", "pinotdb.sqlalchemy", "PinotDialect")

    engine = create_engine(
        "pinot://localhost:8000/query/sql?controller=http://localhost:9000/"
    )  # uses HTTP by default :(
    # engine = create_engine('pinot+http://localhost:8000/query/sql?controller=http://localhost:9000/')
    # engine = create_engine('pinot+https://localhost:8000/query/sql?controller=http://localhost:9000/')

    githubEvents = Table("githubEvents", MetaData(bind=engine), autoload=True)
    print(f"\nSending Count(*) SQL to Pinot\nResults:")
    query=select([func.count("*")], from_obj=githubEvents)
    print(engine.execute(query).scalar())

    Session = sessionmaker(bind=engine)
    session = Session()
    query = session.select(
        [text("json_extract_scalar(repo, \'$.name\', \'STRING\')"), func.count("*")],
        from_obj=githubEvents,
        whereclause=text("json_match(actor, '\"$.login\"=''LombiqBot''')"),
        group_by=text("1"),
        order_by=text("2 DESC"),
        limit="10",
    )
    print(
        f'\nSending SQL: "SELECT json_extract_scalar(repo, \'$.name\', \'STRING\'), count(*) FROM githubEvents'
        f' WHERE json_match(actor, \'\"$.login\"=''LombiqBot''\') GROUP BY 1 ORDER BY 2 DESC LIMIT 10" to Pinot'
        '\nResults:'
    )
    print(engine.execute(query).fetchall())

    Session = sessionmaker(bind=engine)
    session = Session()
    query = select(["*"],
        from_obj=githubEvents,
        limit="10",
    )
    print(
        f'\nSending SQL: "SELECT * FROM githubEvents LIMIT 10" to Pinot'
        f'\nResults:'
    )
    print(engine.execute(query).fetchall())

    Session = sessionmaker(bind=engine)
    session = Session()
    query = select([column("created_at_timestamp")],
        from_obj=githubEvents,
        limit="10",
    )
    print(
        f'\nSending SQL: "SELECT created_at_timestamp FROM githubEvents LIMIT 10" to Pinot'
        f'\nResults:'
    )
    print(engine.execute(query).fetchall())


def run_main():
    run_quickstart_json_batch_example()
    run_quickstart_json_batch_sqlalchemy_example()


if __name__ == '__main__':
    run_main()
