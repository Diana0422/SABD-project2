dir=$(pwd)
### Compiling the kafka program
#mvn -pl sabd2-kafka package
### Compiling the flink program
#mvn -pl sabd2-flink package

# Creating Consumer and Producer docker images
mkdir -p "$dir"/sabd2-kafka/consumer/target/
cp "$dir"/sabd2-kafka/target/sabd2-kafka-1.0-jar-with-dependencies.jar "$dir"/sabd2-kafka/consumer/target/