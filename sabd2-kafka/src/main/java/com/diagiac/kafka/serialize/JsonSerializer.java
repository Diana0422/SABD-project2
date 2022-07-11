package com.diagiac.kafka.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serialize a generic object to a string of bytes
 * @param <T>
 */
public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private byte[] ser;
    public JsonSerializer(){}

    @Override
    public void configure(Map configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Object o) {
        if (o == null) {
            return null;
        }
        try {
            ser = objectMapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing JSON message");
        }
        return ser;
    }

    @Override
    public void close() {}
}
