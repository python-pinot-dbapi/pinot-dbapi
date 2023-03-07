from pinotdb import connect

## Start Pinot Quickstart Batch
## docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 \
##    -d apachepinot/pinot:latest QuickStart -type MULTI_STAGE

def run_pinot_quickstart_batch_example() -> None:
    conn = connect(host="localhost", port=8000, path="/query/sql", scheme="http", use_multistage_engine=True)
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


def run_main():
    run_pinot_quickstart_batch_example()


if __name__ == "__main__":
    run_main()
