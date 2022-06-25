package com.diagiac.kafka;

import com.diagiac.kafka.bean.SensorDataModel;
import com.diagiac.kafka.serialize.JsonSerializer;
import com.diagiac.kafka.utils.ReadCsv;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class SensorProducer {

    public static final Logger logger = Logger.getLogger(SensorProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {

        if (args.length != 1){
            System.out.println(Arrays.toString(args));
            System.out.println("Must add dataset argument");
            return;
        }

        //properties for producer:
        // Recover data from file https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip and send each row
        // to the Kafka broker, so that the Kafka consumer can poll from the topic
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka//kafka:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer"); // TODO forse cambio
        props.put("value.serializer", JsonSerializer.class); // TODO forse cambio

        //create producer
        try (Producer<Integer, SensorDataModel> producer = new KafkaProducer<>(props)) {
            logger.info("Producer is created... ");

            // Read data from file https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip
            // ReadCsv readCsv = new ReadCsv("data/2022-05_bmp180.csv");
            ReadCsv readCsv = new ReadCsv(args[0]);
            List<SensorDataModel> recordList = readCsv.readCSVFile();
            List<SensorDataModel> orderedList = orderByTimestamp(recordList);

            //send messages to my-topic
            int j = 1;
            for (int i = 1; i < orderedList.size(); i++) {
                SensorDataModel data1 = orderedList.get(i - 1);
                SensorDataModel data2 = orderedList.get(i);
                Timestamp ts1 = Timestamp.valueOf(data1.getTimestamp().replace("T", " "));
                Timestamp ts2 = Timestamp.valueOf(data2.getTimestamp().replace("T", " "));
                long timeDiff = ts2.getTime() - ts1.getTime();

                // send data1 and wait diff time to continue the loop
                var producerRecord = new ProducerRecord<>("input-records", j, data1);
                producer.send(producerRecord);
                System.out.printf("Send: %d - %s%n", j, data1);
                producer.flush();
                Thread.sleep(timeDiff / 5_000_000); //TODO pesare in maniera da velocizzare il processamento
                j++;
            }
        } catch(NullPointerException n){
            n.printStackTrace();
        }
    }


    private static List<SensorDataModel> orderByTimestamp(List<SensorDataModel> recordList) {
        recordList.sort((o1, o2) -> {
            Timestamp ts1 = Timestamp.valueOf(o1.getTimestamp().replace("T", " "));
            Timestamp ts2 = Timestamp.valueOf(o2.getTimestamp().replace("T", " "));
            return ts1.compareTo(ts2);
        });
        return recordList;
    }
}
