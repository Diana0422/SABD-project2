package com.diagiac.kafka.serialize;

import com.diagiac.kafka.bean.SensorDataModel;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class CustomDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> typeParameterClass;
    private T dataModel;

    public CustomDeserializer(Class<T> typeParameterClass) {
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        this.typeParameterClass = typeParameterClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data.length == 0) {
            return null;
        }
        try {
            dataModel = objectMapper.readValue(data, typeParameterClass);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataModel;
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
