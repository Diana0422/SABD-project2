package com.diagiac.kafka.streams;

import com.diagiac.kafka.bean.SensorDataModel;
import com.diagiac.kafka.serialize.JsonDeserializer;
import com.diagiac.kafka.serialize.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MySerde implements Serde<SensorDataModel> {

    public MySerde() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serde.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serde.super.close();
    }

    @Override
    public Serializer<SensorDataModel> serializer() {
        return new JsonSerializer();
    }

    @Override
    public Deserializer<SensorDataModel> deserializer() {
        return new JsonDeserializer();
    }
}
