#!/bin/bash
# PREPARE ENVIRONMENT
mvn -pl sabd2-flink package

# EXECUTE QUERIES (MANDATORY)
if [[ $1 -eq 1 ]]
then
  echo "Submitting query 1 to Flink"
  docker exec -t -i jobmanager flink run -p $2 -c com.diagiac.flink.query1.Query1 ./sensor-app/sabd2-flink-1.0-jar-with-dependencies.jar
elif [[ $1 -eq 2 ]]
then
  echo "Submitting query 2 to Flink"
  docker exec -t -i jobmanager flink run -p $2 -c com.diagiac.flink.query2.Query2 ./sensor-app/sabd2-flink-1.0-jar-with-dependencies.jar
elif [[ $1 -eq 3 ]]
then
  echo "Submitting query 3 to Flink"
  docker exec -t -i jobmanager flink run -p $2 -c com.diagiac.flink.query3.Query3 ./sensor-app/sabd2-flink-1.0-jar-with-dependencies.jar
else
  echo "Usage: ./scripts/submit-job.sh query_num parallelism_level"
fi