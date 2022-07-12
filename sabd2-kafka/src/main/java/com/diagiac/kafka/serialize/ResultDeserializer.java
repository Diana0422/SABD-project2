package com.diagiac.kafka.serialize;

import com.diagiac.kafka.streams.bean.AvgResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class ResultDeserializer implements Deserializer<AvgResult> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    AvgResult data;
    public ResultDeserializer(){}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public AvgResult deserialize(String s, byte[] bytes) {
        if (bytes.length == 0) {
            return null;
        }
        try {
            data = objectMapper.readValue(bytes, AvgResult.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    @Override
    public AvgResult deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
