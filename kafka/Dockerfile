FROM eclipse-temurin:11
RUN mkdir /opt/kafka-app/src/main/java/com/diagiac/kafka
COPY kafka-pom.xml /opt/kafka-app/pom.xml
COPY ../src/main/java/com/diagiac/kafka /opt/kafka-app/src/main/java/com/diagiac/kafka
RUN mvn compile
CMD ["java", "/opt/kafka-app/src/main/java/com/diagiac/kafka/SensorProducer"]