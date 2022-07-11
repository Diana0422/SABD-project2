@echo off

@rem if argument 1 is number in 1-4, then execute the query
if "%1" == "1" GOTO q1
if "%1" == "2" GOTO q2
if "%1" == "3" GOTO q3
if "%1" == "4" GOTO q4

@rem otherwise close and show error message
echo "Usage: ./scripts/submit_job.sh <query_num in 1-4> <parallelism_level 1-4>"
GOTO done

:q1
echo Submitting query 1 to Flink
docker exec -t -i jobmanager flink run -p "%2" -c com.diagiac.flink.query1.Query1 ./sensor-app/sabd2-flink-1.0-jar-with-dependencies.jar "kafka://kafka:9092"
goto done

:q2
echo Submitting query 2 to Flink
docker exec -t -i jobmanager flink run -p "%2" -c com.diagiac.flink.query2.Query2 ./sensor-app/sabd2-flink-1.0-jar-with-dependencies.jar "kafka://kafka:9092"
goto done

:q3
echo Submitting query 3 to Flink
docker exec -t -i jobmanager flink run -p "%2" -c com.diagiac.flink.query3.Query3 ./sensor-app/sabd2-flink-1.0-jar-with-dependencies.jar "kafka://kafka:9092"
goto done

:q4
echo Submitting query 1 to KafkaStreams
docker exec -t -i kafka java -cp ./target/sabd2-kafka-1.0-jar-with-dependencies.jar com.diagiac.kafka.streams.Query1KafkaStreams
goto done

:done
exit