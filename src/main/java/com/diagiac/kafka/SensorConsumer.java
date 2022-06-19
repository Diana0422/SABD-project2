package com.diagiac.kafka;

import com.diagiac.kafka.bean.BlaBean;
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
        KafkaConsumer<Integer, BlaBean> consumer = new KafkaConsumer<Integer, BlaBean>(props);

        //subscribe to topic
        consumer.subscribe(Arrays.asList("input-records"));

        //infinite poll loop
        while (true) {
            ConsumerRecords<Integer, BlaBean> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, BlaBean> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value().toString());
        }
    }
}
