package com.diagiac.flink.query1;

import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query1.bean.Query1Result;
import com.diagiac.flink.RedisHashSink;

/**
 * This sink is used to save query1Results to a redis Hash set.
 * Each hashset only have three fields: timestamp, count, averageTemperatures
 */
public class RedisHashSink1 extends RedisHashSink<Query1Result> {

    private final WindowEnum windowType;

    public RedisHashSink1(WindowEnum windowType) {
        this.windowType = windowType;
    }

    @Override
    public void setHashFieldsFrom(Query1Result flinkResult) {
        setHashField(flinkResult.getRedisKey(windowType), "timestamp", flinkResult.getTimestamp());
        setHashField(flinkResult.getRedisKey(windowType), "count", flinkResult.getCount());
        setHashField(flinkResult.getRedisKey(windowType), "averageTemperature", flinkResult.getAvgTemperature());
    }
}
