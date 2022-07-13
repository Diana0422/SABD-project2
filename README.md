# SABD-project2
This repository contains a code base that permits the execution of a real time analysis of environmental data recovered by Sensor.Community sensors. The main objective is to answer the following queries:

**Query1**: For those sensors having sensor_id < 10000, find the number of measurements and the temperature average value.
Using a tumbling window, calculate this query:
* every 1 hour (event time)
* every 1 week (event time)
* from the beginning of the dataset

**Query2**: Find the real-time top-5 ranking of locations (location) having the highest average temperature and the top-5 ranking of locations (location) having the lowest average temperature.
Using a tumbling window, calculate this query:
* every 1 hour (event time)
* every 1 day (event time)
* every 1 week (event time)

**Query3**: Consider the latitude and longitude coordinates within the geographic area which is identified from the latitude and longitude coordinates (38°, 2°) and (58°, 30°). Divide this area using a 4x4 grid and identify each grid cell from the top-left to bottom-right corners using the name "cell_X", where X is the cell id from 0 to 15. For each cell, find the average and the median temperature, taking into account the values emitted from the sensors which are located inside that cell.
Using a tumbling window, calculate this query:
* every 1 hour (event time)
* every 1 day (event time)
* every 1 week (event time

The code for the queries can be found in the codebase at these directories:

* sabd2-flink/src/main/java/com/diagiac/flink/query1/Query1.java
* sabd2-flink/src/main/java/com/diagiac/flink/query2/Query2.java
* sabd2-flink/src/main/java/com/diagiac/flink/query3/Query3.java

We also implemented alternate of the query 1 using Kafka Streams and the implementation can be found at:

* sabd2-kafka/src/main/java/com/diagiac/kafka/streams/Query1KafkaStreams.java

## Requirements
This project uses **Docker** and **DockerCompose** to instantiate the Kafka, Zookeeper, Flink, Grafana, Prometheus, Redis and Publisher/Consumer containers.
* [get docker](https://docs.docker.com/get-docker/)
* [install docker compose](https://docs.docker.com/compose/install/)

## Deployment
> **_NOTE:_** First you need to compile the project using **Maven**. Open a terminal in the project base directory and execute the following command
``` 
mvn clean package
```
> **_NOTE:_** Also some initialization ops are needed before the deploymento of the application, so execute this command in the project base directory
``` 
./scripts/init.sh
```
To deploy this project use **DockerCompose**:
``` 
docker compose up -d
```

## Execute Query:
> **_NOTE:_** Do this after the [deployment phase](##Deployment:). Also you need to wait for the Producer to begin its work.
Open a terminal in the project base directory and follow these steps:
* Bash:
```
./scripts/submit_job.sh <num-query> <parallelization-level>
```
* Shell:
```
.\scripts\submit_job.cmd <num-query> <parallelization-level>
```
where `num-query` is the number of the query to execute:
 - 1 -> Query1
 - 2 -> Query2
 - 3 -> Query3
 - 4 -> Query1KafkaStreams
 
 ## UIs:
* **Flink**: http://localhost:8081/
* **Prometheus**: http://localhost:9090/graph
* **Grafana**: http://localhost:8000/

You can find the **Grafana Dashboards** of the project at:
* **performances**: http://localhost:8000/d/UDdpyzz7z/prometheus-2-0-stats?orgId=1&refresh=5s
* **query results**: http://localhost:8000/d/2J8ln097k/sabd-project-2?orgId=1&var-redis=Redis-Cache&var-sensor=8762&var-window=Hour

## Frameworks:
* **Kafka**: used for data ingestion
* **Flink**: used for data stream processing
* **Prometheus**: used as data storage for query metrics.
* **Redis**: used to cache output data to be read by the data visualization layer.
* **Grafana**: used to visualize output data after processing and performances from Prometheus.

<a href="https://nightlies.apache.org/flink/flink-docs-stable/">
<img src="https://www.pinclipart.com/picdir/big/523-5236504_apache-flink-clipart.png" width="150" height="150">
</a>
<a href="https://kafka.apache.org/24/documentation.html">
<img src="https://iconape.com/wp-content/files/vq/370992/svg/370992.svg" width="150" height="150">
</a>
<a href="https://redis.io">
<img src="https://static.cdnlogo.com/logos/r/31/redis.svg" width="150" height="150">
</a>
<a href="https://prometheus.io/docs/introduction/overview/">
<img src="https://cdn.freebiesupply.com/logos/thumbs/2x/prometheus-logo.png" width="200" height="150">
</a>
<a href="https://grafana.com">
<img src="https://cdn.worldvectorlogo.com/logos/grafana.svg" width="150" height="150">
</a>
