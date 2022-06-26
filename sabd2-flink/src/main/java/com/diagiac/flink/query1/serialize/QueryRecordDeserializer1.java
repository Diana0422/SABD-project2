package com.diagiac.flink.query1.serialize;

import com.diagiac.flink.FlinkRecord;
import com.diagiac.flink.query1.bean.Query1Record;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class QueryRecordDeserializer1 implements Deserializer<Query1Record> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Query1Record deserialize(String s, byte[] bytes) {
        String rawMessage = new String(bytes, StandardCharsets.UTF_8);
        return Query1Record.create(rawMessage);
    }

    @Override
    public Query1Record deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
