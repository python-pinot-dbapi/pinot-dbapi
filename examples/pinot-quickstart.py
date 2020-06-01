from pinotdb import connect

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batch
conn = connect(host='localhost', port=8000, path='/query/sql', scheme='http')
curs = conn.cursor()
sql = "SELECT * FROM baseballStats LIMIT 5"
print(f'Sending SQL to Pinot: {sql}')
curs.execute(sql)
for row in curs:
    print(row)

sql = "SELECT playerName, sum(runs) FROM baseballStats WHERE yearID>=2000 GROUP BY playerName LIMIT 5"
print(f'\nSending SQL to Pinot: {sql}')
curs.execute(sql)
for row in curs:
    print(row)

sql = "SELECT playerName,sum(runs) AS sum_runs FROM baseballStats WHERE yearID>=2000 GROUP BY playerName ORDER BY sum_runs DESC LIMIT 5"
print(f'\nSending SQL to Pinot: {sql}')
curs.execute(sql)
for row in curs:
    print(row)
