package com.diagiac.kafka;

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
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * It writes each record in input on a different file based on the topic from which it has read it
 */
public class ResultConsumer {
    private static final String query1Header = "ts,sensor_id,count,avg\n";
    private static final String query2Header = "ts,sea,slot_a,rank_a,slot_p,rank_p\n";
    private static final String query3Header = "ts,trip_1,rating_1,trip_2,rating_2,trip_3,rating_3,trip_4,rating_4,trip_5,rating_5\n";
    private static final Map<String, String> headers;
    private static final String outputPath = "/Results";

    static {
        headers = new HashMap<>();
        headers.put("query1-header", query1Header);
        headers.put("query2-header", query2Header);
        headers.put("query3-header", query3Header);
    }

    public static void main(String[] args) {
        Logger log = Logger.getLogger(Consumer.class.getSimpleName());
        var url = (args.length > 1 ? args[1] : "127.0.0.1:29092");

        Properties props = new Properties();
        props.put("bootstrap.servers", url);
        props.put("group.id", "test-group");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", StringDeserializer.class);
        org.apache.kafka.clients.consumer.Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        Map<String, FileWriter> topicWriterMap = new HashMap<>();
        consumer.subscribe(Pattern.compile("^(query).*$"));

        log.info("Starting receiving records");
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
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
                            fileWriter.write(headers.get(topic.substring(0,6)));
                            fileWriter.flush();
                            topicWriterMap.put(topic, fileWriter);
                        }
                        fileWriter.write(longStringConsumerRecord.value() + "\n");

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
