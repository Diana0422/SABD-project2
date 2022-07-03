package com.diagiac.flink.query1;

import com.diagiac.flink.query1.bean.Query1Result;
import com.diagiac.flink.RedisHashSink;

public class RedisHashSink1 extends RedisHashSink<Query1Result> {
    @Override
    public void setHashFieldsFrom(Query1Result flinkResult) {
        setHashField(flinkResult.getKey(), "timestamp", flinkResult.getTimestamp());
        setHashField(flinkResult.getKey(), "count", flinkResult.getCount());
        setHashField(flinkResult.getKey(), "averageTemperature", flinkResult.getAvgTemperature());
    }
}
