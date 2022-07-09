package com.diagiac.flink.query2.serialize;

import com.diagiac.flink.query2.bean.Query2Record;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Deserializes records from a String to a Query2Record. Used in kafka source for query2.
 */
public class QueryRecordDeserializer2 implements Deserializer<Query2Record> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Query2Record deserialize(String s, byte[] bytes) {
        String rawMessage = new String(bytes, StandardCharsets.UTF_8);
        return Query2Record.create(rawMessage);
    }

    @Override
    public Query2Record deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
