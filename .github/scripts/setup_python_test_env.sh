#!/bin/bash



### ---------------------------------------------------------------------------
### Install python packages
### ---------------------------------------------------------------------------

echo "Install Python Packages"

pip install -r ./requirements.txt
pip install -U urllib3


### ---------------------------------------------------------------------------
### Ensure Pinot cluster started correctly.
### ---------------------------------------------------------------------------

echo "Ensure Pinot cluster started correctly"

# Wait at most 5 minutes to reach the desired state
for i in $(seq 1 150)
do
  SUCCEED_TABLE=0
  for table in "airlineStats" "baseballStats" "dimBaseballTeams" "githubComplexTypeEvents" "githubEvents" "starbucksStores";
  do
    QUERY="select count(*) from ${table} limit 1"
    QUERY_REQUEST='curl -s -X POST -H '"'"'Accept: application/json'"'"' -d '"'"'{"sql": "'${QUERY}'"}'"'"' http://localhost:'${BROKER_PORT_FORWARD}'/query/sql'
    echo ${QUERY_REQUEST}
    QUERY_RES=`eval ${QUERY_REQUEST}`
    echo ${QUERY_RES}
    
    if [ $? -eq 0 ]; then
      COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
      if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -gt 0 ]; then
        SUCCEED_TABLE=$((SUCCEED_TABLE+1))
      fi
    fi
    echo "QUERY: ${QUERY}, QUERY_RES: ${QUERY_RES}"
  done
  echo "SUCCEED_TABLE: ${SUCCEED_TABLE}"
  if [ "${SUCCEED_TABLE}" -eq 6 ]; then
    break
  fi
  sleep 2
done

if [ "${SUCCEED_TABLE}" -lt 6 ]; then
  echo 'Quickstart failed: Cannot confirmed count-star result from quickstart table in 5 minutes'
  exit 1
fi
echo "Pinot cluster started correctly"
