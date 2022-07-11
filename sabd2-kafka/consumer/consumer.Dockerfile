FROM eclipse-temurin:11-alpine
COPY ../target/sabd2-kafka-1.0-jar-with-dependencies.jar .
RUN mkdir "output"
CMD ["java", "-cp", "sabd2-kafka-1.0-jar-with-dependencies.jar", "com.diagiac.kafka.ResultConsumer", "kafka://kafka:9092"]