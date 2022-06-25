package com.diagiac.flink.query1.serialize;

import com.diagiac.flink.query1.bean.Query1Record;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class InputMessageDeserializationSchema implements DeserializationSchema<Query1Record> {

    @Override
    public Query1Record deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(Query1Record query1Record) {
        return false;
    }

    @Override
    public TypeInformation<Query1Record> getProducedType() {
        return null;
    }
}
