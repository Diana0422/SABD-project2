package com.diagiac.kafka.streams.serde;

import com.diagiac.kafka.serialize.CustomDeserializer;
import com.diagiac.kafka.serialize.JsonSerializer;
import com.diagiac.kafka.streams.bean.CountAndSum;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class AvgCountSerde implements Serde<CountAndSum> {

    public AvgCountSerde() {}
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serde.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serde.super.close();
    }

    @Override
    public Serializer<CountAndSum> serializer() {
        return new JsonSerializer<>();
    }

    @Override
    public Deserializer<CountAndSum> deserializer() {
        return new CustomDeserializer<>(CountAndSum.class);
    }
}
