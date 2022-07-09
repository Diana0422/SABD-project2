package com.diagiac.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * It writes each record in input on a different file based on the topic from which it has read it
 * The output file is a CSV!!!
 */
public class ResultConsumer {
    private static final String query1Header = "ts,sensor_id,count,avg\n";
    private static final String query2Header = "ts,location1,avg_temp1,location2,avg_temp2,location3,avg_temp3,location4,avg_temp4,location5,avg_temp5,location6,avg_temp6,location7,avg_temp7,location8,avg_temp8,location9,avg_temp9,location10,avg_temp10\n";
    private static final String query3Header = "ts,cell_0,avg_temp0,med_temp0,cell_1,avg_temp1,med_temp1,cell_2,avg_temp2,med_temp2,cell_3,avg_temp3,med_temp3,cell_4,avg_temp4,med_temp4,cell_5,avg_temp5,med_temp5,cell_6,avg_temp6,med_temp6,cell_7,avg_temp7,med_temp7,cell_8,avg_temp8,med_temp8,cell_9,avg_temp9,med_temp9,cell_10,avg_temp10,med_temp10,cell_11,avg_temp11,med_temp11,cell_12,avg_temp12,med_temp12,cell_13,avg_temp13,med_temp13,cell_14,avg_temp14,med_temp14,cell_15,avg_temp15,med_temp15\n";
    private static final Map<String, String> headers;
    private static final String outputPath = "/output";

    static {
        headers = new HashMap<>();
        headers.put("query1-header", query1Header);
        headers.put("query2-header", query2Header);
        headers.put("query3-header", query3Header);
    }

    public static void main(String[] args) {
        Logger log = Logger.getLogger(ResultConsumer.class.getSimpleName());
        var url = "kafka://kafka:9092";

        Properties props = new Properties();
        props.put("bootstrap.servers", url);
        props.put("group.id", "consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", StringDeserializer.class);
        try(Consumer<Long, String> consumer = new KafkaConsumer<>(props)){
            Map<String, FileWriter> topicWriterMap = new HashMap<>();
            consumer.subscribe(Pattern.compile("^(query).*$"));

            log.info("Starting receiving records");
            while (true) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (consumerRecords.count() == 0) {
                    log.fine("No records");
                } else {
                    consumerRecords.forEach(longStringConsumerRecord -> {
                        try {
                            String topic = longStringConsumerRecord.topic();
                            log.log(Level.FINER, "Topic: {0} - Record: {1}",
                                    Arrays.asList(topic, longStringConsumerRecord.value()));
                            FileWriter fileWriter = topicWriterMap.get(topic);
                            if (fileWriter == null) {
                                log.log(Level.INFO, "New topic: {0}", topic);
                                fileWriter = new FileWriter(outputPath + "/" + topic + ".csv", false);
                                String key = topic.substring(0,6)+"-header";
                                String header = headers.get(key);
                                fileWriter.write(header);
                                fileWriter.flush();
                                topicWriterMap.put(topic, fileWriter);
                            }
                            fileWriter.write(longStringConsumerRecord.value());

                            fileWriter.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                }
                consumer.commitAsync();
            }
        }

    }
}
