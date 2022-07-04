package com.diagiac.flink.query1;

import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query1.bean.Query1Result;
import com.diagiac.flink.RedisHashSink;

public class RedisHashSink1 extends RedisHashSink<Query1Result> {

    private WindowEnum windowType;

    public RedisHashSink1(WindowEnum windowType) {
        this.windowType = windowType;
    }

    @Override
    public void setHashFieldsFrom(Query1Result flinkResult) {
        setHashField(flinkResult.getKey(windowType), "timestamp", flinkResult.getTimestamp());
        setHashField(flinkResult.getKey(windowType), "count", flinkResult.getCount());
        setHashField(flinkResult.getKey(windowType), "averageTemperature", flinkResult.getAvgTemperature());
    }
}
