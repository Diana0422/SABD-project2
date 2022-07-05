dir=$(pwd)

## Compiling the kafka program
#mvn -pl sabd2-kafka package
## Compiling the flink program
#mvn -pl sabd2-flink package

# Creating Consumer and Producer docker images
mkdir "$dir"/sabd2-kafka/consumer/target/ "$dir"/sabd2-kafka/consumer/output/
#cp "$dir"/target/sabd-project2-1.0-SNAPSHOT-jar-with-dependencies.jar "$dir"/docker-compose/producer/target/
cp "$dir"/sabd2-kafka/target/sabd2-kafka-1.0-jar-with-dependencies.jar "$dir"/sabd2-kafka/consumer/target/
chmod -R go+rwx "$dir"/docker-compose/consumer/output/