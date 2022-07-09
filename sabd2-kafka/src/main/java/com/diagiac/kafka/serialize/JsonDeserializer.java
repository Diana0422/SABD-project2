package com.diagiac.kafka.serialize;

import com.diagiac.kafka.bean.SensorDataModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Unused. Can be used to deserialize a JSON string into a SensorDataModel
 */
public class JsonDeserializer implements Deserializer<SensorDataModel> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    SensorDataModel dataModel;
    @Override
    public void configure(Map configs, boolean isKey) {}

    @Override
    public SensorDataModel deserialize(String s, byte[] bytes) {
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
