FROM eclipse-temurin:11-alpine

## Install gzip
RUN apk --update --no-cache add gzip # curl

## Creating directory for our kafka publisher and copying from host
RUN mkdir /opt/kafka-app
WORKDIR /opt/kafka-app

# Download dataset to simulate real-time data
COPY 2022-05_bmp180.csv.gz .
RUN gzip -d 2022-05_bmp180.csv.gz

COPY ./target/sabd2-kafka-1.0-jar-with-dependencies.jar .
RUN docker exec kafka kafka-topics --delete --zookeeper zookeeper:2181 --topic input-records
# compile and run
CMD ["java", "-jar", "sabd2-kafka-1.0-jar-with-dependencies.jar", "2022-05_bmp180.csv", "kafka://kafka:9092", "5000000"]