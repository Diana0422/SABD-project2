package com.diagiac.flink.query3.serialize;

import com.diagiac.flink.query3.bean.Query3Record;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class QueryRecordDeserializer3 implements Deserializer<Query3Record> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Query3Record deserialize(String s, byte[] bytes) {
        String rawMessage = new String(bytes, StandardCharsets.UTF_8);
        return Query3Record.create(rawMessage);
    }

    @Override
    public Query3Record deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
