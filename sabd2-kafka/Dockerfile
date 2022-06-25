FROM eclipse-temurin:11-alpine
## Variables used later
#ARG MAVEN_VERSION=3.6.3
#ARG USER_HOME_DIR="/root"
#ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries
#
## Install gzip
RUN apk --update --no-cache add gzip # curl
#
## Download, extract, and link maven from website
#RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
# && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
# && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
# && rm -f /tmp/apache-maven.tar.gz \
# && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn
#
## Setting maven environment variables
#ENV MAVEN_HOME /usr/share/maven
#ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"
#
## Creating directory for our kafka publisher and copying from host
RUN mkdir /opt/kafka-app
WORKDIR /opt/kafka-app
#COPY src/ ./src
#COPY pom.xml .

# Download dataset to simulate real-time data
COPY 2022-05_bmp180.csv.gz .
RUN gzip -d 2022-05_bmp180.csv.gz

COPY ./target/sabd2-kafka-1.0-jar-with-dependencies.jar .
# compile and run
CMD ["java", "-jar", "sabd2-kafka-1.0-jar-with-dependencies.jar", "2022-05_bmp180.csv"]