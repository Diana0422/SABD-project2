FROM eclipse-temurin:11-alpine

## Creating directory for our kafka publisher and copying from host
RUN mkdir /opt/kafka-app
WORKDIR /opt/kafka-app

COPY ./target/sabd2-kafka-1.0-jar-with-dependencies.jar .
#CMD ["java", "-cp", "sabd2-kafka-1.0-jar-with-dependencies.jar", "2022-05_bmp180.csv", "kafka://kafka:9092", "5000000"]
CMD java -cp sabd2-kafka-1.0-jar-with-dependencies.jar com.diagiac.kafka.ResultConsumer