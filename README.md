# Python DB-API and SQLAlchemy dialect for Pinot #

This module allows accessing Pinot via its [SQL API](https://github.com/linkedin/pinot/wiki/Pinot-Query-Language).

## Usage ##

Using the DB API:

```python
from pinotdb import connect

conn = connect(host='localhost', port=8099, path='/query', scheme='http')
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

engine = create_engine('pinot://localhost:8099/query?server=http://localhost:9000/')  # uses HTTP by default :(
# engine = create_engine('pinot+http://localhost:8099/query?server=http://localhost:9000/')
# engine = create_engine('pinot+https://localhost:8099/query?server=http://localhost:9000/')

places = Table('places', MetaData(bind=engine), autoload=True)
print(select([func.count('*')], from_obj=places).scalar())
```

