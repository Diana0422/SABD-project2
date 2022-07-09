package com.diagiac.flink.query2.serialize;

import com.diagiac.flink.query2.bean.Query2Result;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Serializes query2record to string for kafka sink.
 */
public class QueryResultSerializer2 implements Serializer<Query2Result> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, Query2Result query2Result) {
        return query2Result.toStringCSV().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Query2Result data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
