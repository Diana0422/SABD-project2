package com.diagiac.kafka;

import com.diagiac.kafka.bean.SensorDataModel;
import com.diagiac.kafka.serialize.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SensorConsumer {
    public static void main(String[] args) {
        //consumer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");

        //using auto commit
        props.put("enable.auto.commit", "true");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", JsonDeserializer.class);

        //kafka consumer object
        KafkaConsumer<Integer, SensorDataModel> consumer = new KafkaConsumer<Integer, SensorDataModel>(props);

        //subscribe to topic
        consumer.subscribe(Arrays.asList("input-records"));

        //infinite poll loop
        while (true) {
            ConsumerRecords<Integer, SensorDataModel> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, SensorDataModel> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value().toString());
        }
    }
}
