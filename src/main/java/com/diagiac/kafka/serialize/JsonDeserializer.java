package com.diagiac.kafka.serialize;

import com.diagiac.kafka.bean.SensorDataModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    SensorDataModel dataModel;
    @Override
    public void configure(Map configs, boolean isKey) {}

    @Override
    public Object deserialize(String s, byte[] bytes) {
        if (bytes.length == 0) {
            return null;
        }
        try {
            dataModel = objectMapper.readValue(bytes, SensorDataModel.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataModel;
    }
}
