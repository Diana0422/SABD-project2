package com.diagiac.flink.redis;

import com.diagiac.flink.query1.bean.Query1Result;

public class RedisMapper1 implements RedisTSMapper<Query1Result> {

    @Override
    public String getKeyFromData(Query1Result data) {
        return data.getSensorId().toString();
    }

    @Override
    public String getValueFromData(Query1Result data) {
        return data.getAvgTemperature().toString();
    }

    @Override
    public String getTimestampFromData(Query1Result data) {
        return String.valueOf(data.getTimestamp().getTime());
    }
}
