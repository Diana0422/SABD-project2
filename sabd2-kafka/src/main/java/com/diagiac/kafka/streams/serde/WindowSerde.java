package com.diagiac.kafka.streams.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Map;

public class WindowSerde implements Serde<Windowed<Long>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serde.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serde.super.close();
    }

    @Override
    public Serializer<Windowed<Long>> serializer() {
        return new TimeWindowedSerializer<>(Serdes.Long().serializer());
    }

    @Override
    public Deserializer<Windowed<Long>> deserializer() {
        return new TimeWindowedDeserializer<>();
    }
}
