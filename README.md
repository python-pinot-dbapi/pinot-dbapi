# Python DB-API and SQLAlchemy dialect for Pinot #

This module allows accessing Pinot via its [SQL API](https://github.com/linkedin/pinot/wiki/Pinot-Query-Language).

## Usage ##

Using the DB API:

```python
from pinotdb import connect

conn = connect(host='localhost', port=8099, path='/query/sql', scheme='http')
curs = conn.cursor()
curs.execute("""
    SELECT place,
           CAST(REGEXP_EXTRACT(place, '(.*),', 1) AS FLOAT) AS lat,
           CAST(REGEXP_EXTRACT(place, ',(.*)', 1) AS FLOAT) AS lon
      FROM places
     LIMIT 10
""")
for row in curs:
    print(row)
```
        
Using SQLAlchemy:

```python
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

engine = create_engine('pinot://localhost:8099/query/sql?server=http://localhost:9000/')  # uses HTTP by default :(
# engine = create_engine('pinot+http://localhost:8099/query/sql?server=http://localhost:9000/')
# engine = create_engine('pinot+https://localhost:8099/query/sql?server=http://localhost:9000/')

places = Table('places', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=places).scalar())
```

## Examples with Pinot Quickstart ##

Start Pinot Batch Quickstart

```bash
docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batch
```

Once pinot-quickstart is up, you can run below sample code snippet to query Pinot:

```bash
python3 examples/pinot-quickstart.py
```

Sample Output:
```
Sending SQL to Pinot: SELECT * FROM baseballStats LIMIT 5
[0, 11, 0, 0, 0, 0, 0, 0, 0, 0, '"NL"', 11, 11, '"aardsda01"', '"David Allan"', 1, 0, 0, 0, 0, 0, 0, '"SFN"', 0, 2004]
[2, 45, 0, 0, 0, 0, 0, 0, 0, 0, '"NL"', 45, 43, '"aardsda01"', '"David Allan"', 1, 0, 0, 0, 1, 0, 0, '"CHN"', 0, 2006]
[0, 2, 0, 0, 0, 0, 0, 0, 0, 0, '"AL"', 25, 2, '"aardsda01"', '"David Allan"', 1, 0, 0, 0, 0, 0, 0, '"CHA"', 0, 2007]
[1, 5, 0, 0, 0, 0, 0, 0, 0, 0, '"AL"', 47, 5, '"aardsda01"', '"David Allan"', 1, 0, 0, 0, 0, 0, 1, '"BOS"', 0, 2008]
[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '"AL"', 73, 3, '"aardsda01"', '"David Allan"', 1, 0, 0, 0, 0, 0, 0, '"SEA"', 0, 2009]

Sending SQL to Pinot: SELECT playerName, sum(runs) FROM baseballStats WHERE yearID>=2000 GROUP BY playerName LIMIT 5
['"Scott Michael"', 26.0]
['"Justin Morgan"', 0.0]
['"Jason Andre"', 0.0]
['"Jeffrey Ellis"', 0.0]
['"Maximiliano R."', 16.0]

Sending SQL to Pinot: SELECT playerName,sum(runs) AS sum_runs FROM baseballStats WHERE yearID>=2000 GROUP BY playerName ORDER BY sum_runs DESC LIMIT 5
['"Adrian"', 1820.0]
['"Jose Antonio"', 1692.0]
['"Rafael"', 1565.0]
['"Brian Michael"', 1500.0]
['"Alexander Emmanuel"', 1426.0]
```

Below is a sql alchemy example:

```bash
python3 examples/pinot-quickstart-sqlalchemy.py
```

```bash
Sending Count(*) SQL to Pinot
97889

Sending SQL: "SELECT playerName, sum(runs) AS sum_runs FROM "baseballStats" WHERE yearID>=2000 GROUP BY playerName ORDER BY sum_runs DESC LIMIT 5" to Pinot
[('"Adrian"', 1820.0), ('"Jose Antonio"', 1692.0), ('"Rafael"', 1565.0), ('"Brian Michael"', 1500.0), ('"Alexander Emmanuel"', 1426.0)]
```
