from pinotdb import connect

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type hybrid
conn = connect(host='localhost', port=8000, path='/query/sql', scheme='http')
curs = conn.cursor()
sql = "SELECT * FROM airlineStats LIMIT 5"
print(f'Sending SQL to Pinot: {sql}')
curs.execute(sql)
for row in curs:
    print(row)

sql = "SELECT count(*) FROM airlineStats LIMIT 5"
print(f'\nSending SQL to Pinot: {sql}')
curs.execute(sql)
for row in curs:
    print(row)

sql = "SELECT AirlineID, sum(Cancelled) FROM airlineStats WHERE Year > 2010 GROUP BY AirlineID LIMIT 5"
print(f'\nSending SQL to Pinot: {sql}')
curs.execute(sql)
for row in curs:
    print(row)

sql = "select OriginCityName, max(Flights) from airlineStats group by OriginCityName ORDER BY max(Flights) DESC LIMIT 5"
print(f'\nSending SQL to Pinot: {sql}')
curs.execute(sql)
for row in curs:
    print(row)

sql = "SELECT OriginCityName, sum(Cancelled) AS sum_cancelled FROM airlineStats WHERE Year>2010 GROUP BY OriginCityName ORDER BY sum_cancelled DESC LIMIT 5"
print(f'\nSending SQL to Pinot: {sql}')
curs.execute(sql)
for row in curs:
    print(row)

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batchpy

engine = create_engine('pinot://localhost:8000/query/sql?server=http://localhost:9000/')  # uses HTTP by default :(
# engine = create_engine('pinot+http://localhost:8000/query/sql?server=http://localhost:9000/')
# engine = create_engine('pinot+https://localhost:8000/query/sql?server=http://localhost:9000/')

airlineStats = Table('airlineStats', MetaData(bind=engine), autoload=True)
print(f'\nSending Count(*) SQL to Pinot')
print(select([func.count('*')], from_obj=airlineStats).scalar())


from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()
query = select([column('OriginCityName'), func.sum(column('Cancelled')).label('sum_cancelled')], from_obj=airlineStats,
                   whereclause=text('Year>2010'),
                   group_by=[column('OriginCityName')],
                   order_by=text('sum_cancelled DESC'),
                   limit='5')
print(f'\nSending SQL: "SELECT OriginCityName, sum(Cancelled) AS sum_cancelled FROM "airlineStats" WHERE Year>2010 GROUP BY OriginCityName ORDER BY sum_cancelled DESC LIMIT 5" to Pinot')
print(engine.execute(query).fetchall())
