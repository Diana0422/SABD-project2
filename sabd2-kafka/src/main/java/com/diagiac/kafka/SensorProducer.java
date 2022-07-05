package com.diagiac.kafka;

import com.diagiac.kafka.bean.SensorDataModel;
import com.diagiac.kafka.serialize.JsonSerializer;
import com.diagiac.kafka.utils.ReadCsv;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class SensorProducer {

    public static final Logger logger = Logger.getLogger(SensorProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {

        // TODO: leggere i file da conf.properties
        var dataset = (args.length > 0 ? args[0] : "sabd2-kafka/2022-05_bmp180.csv");
        if (!new File(dataset).exists()) {
            System.out.println("The dataset doesn't exists, download and extract it from https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip");
            return;
        }
        var url = (args.length > 1 ? args[1] : "127.0.0.1:29092");

        var speedup = args.length > 2 ? Integer.parseInt(args[2]) : 5_000_000;

        //properties for producer:
        // Recover data from file https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip and send each row
        // to the Kafka broker, so that the Kafka consumer can poll from the topic
        Properties props = new Properties();
        props.put("broker.list", url);
        props.put("bootstrap.servers", url);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer"); // TODO forse cambio
        props.put("value.serializer", JsonSerializer.class); // TODO forse cambio
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        //create producer
        try (Producer<Integer, SensorDataModel> producer = new KafkaProducer<>(props)) {
            logger.info("Producer is created... ");

            // Read data from file https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip
            // ReadCsv readCsv = new ReadCsv("data/2022-05_bmp180.csv");
            ReadCsv readCsv = new ReadCsv(dataset);
            System.out.println("Start reading and ordering dataset");
            List<SensorDataModel> recordList = readCsv.readCSVFile();
            List<SensorDataModel> orderedList = orderByTimestamp(recordList);
            System.out.println("Start publishing");
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
                if (j % 10000 == 0){
                    System.out.printf("Sent %d: %s%n", j, data1);
                }
                producer.flush();
                Thread.sleep(timeDiff / speedup); // weighted to speed up the producer
                j++;
            }
        } catch (NullPointerException n) {
            n.printStackTrace();
        }
        System.out.println("FINISHED PUBLISHING");
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
