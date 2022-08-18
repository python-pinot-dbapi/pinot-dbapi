#!/bin/bash



### ---------------------------------------------------------------------------
### Install python packages
### ---------------------------------------------------------------------------

echo "Install Python Packages"

pip install -r ./requirements.txt

### ---------------------------------------------------------------------------
### Ensure Pinot cluster started correctly.
### ---------------------------------------------------------------------------

echo "Ensure Pinot cluster started correctly"

# Wait at most 5 minutes to reach the desired state
PASS=0

for i in $(seq 1 150)
do
  QUERY_RES=`curl -X POST --header 'Accept: application/json' -d '{"sql":"select count(*) from baseballStats limit 1"}' http://localhost:${BROKER_PORT_FORWARD}/query/sql`
  if [ $? -eq 0 ]; then
    COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
    if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -gt 0 ]; then
      PASS=1
      break
    fi
  fi
  echo "QUERY_RES: ${QUERY_RES}"
  sleep 2
done

if [ "${PASS}" -eq 0 ]; then
  echo 'Quickstart failed: Cannot confirmed count-star result from quickstart table in 5 minutes'
  exit 1
fi
echo "Pinot cluster started correctly"