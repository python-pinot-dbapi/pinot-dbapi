from pinotdb import connect

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batch
conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http")
curs = conn.cursor()
sql = "SELECT * FROM baseballStats LIMIT 5"
print(f"Sending SQL to Pinot: {sql}")
curs.execute(sql)
for row in curs:
    print(row)

sql = "SELECT playerName, sum(runs) FROM baseballStats WHERE yearID>=2000 GROUP BY playerName LIMIT 5"
print(f"\nSending SQL to Pinot: {sql}")
curs.execute(sql)
for row in curs:
    print(row)

sql = "SELECT playerName,sum(runs) AS sum_runs FROM baseballStats WHERE yearID>=2000 GROUP BY playerName ORDER BY sum_runs DESC LIMIT 5"
print(f"\nSending SQL to Pinot: {sql}")
curs.execute(sql)
for row in curs:
    print(row)

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batchpy

engine = create_engine(
    "pinot://localhost:8000/query/sql?controller=http://localhost:9000/"
)  # uses HTTP by default :(
# engine = create_engine('pinot+http://localhost:8000/query/sql?controller=http://localhost:9000/')
# engine = create_engine('pinot+https://localhost:8000/query/sql?controller=http://localhost:9000/')

baseballStats = Table("baseballStats", MetaData(bind=engine), autoload=True)
print(f"\nSending Count(*) SQL to Pinot")
query=select([func.count("*")], from_obj=baseballStats)
print(engine.execute(query).scalar())


from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session = Session()
query = select(
    [column("playerName"), func.sum(column("runs")).label("sum_runs")],
    from_obj=baseballStats,
    whereclause=text("yearID>=2000"),
    group_by=[column("playerName")],
    order_by=text("sum_runs DESC"),
    limit="5",
)
print(
    f'\nSending SQL: "SELECT playerName, sum(runs) AS sum_runs FROM "baseballStats" WHERE yearID>=2000 GROUP BY playerName ORDER BY sum_runs DESC LIMIT 5" to Pinot'
)
print(engine.execute(query).fetchall())
