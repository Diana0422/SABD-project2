package com.diagiac.flink.query3.serialize;

import com.diagiac.flink.query3.bean.Query3Result;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Serializes query2record to string for kafka sink.
 */
public class QueryResultSerializer3 implements Serializer<Query3Result> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, Query3Result query3Result) {
        return query3Result.toStringCSV().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Query3Result data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
