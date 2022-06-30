package com.diagiac.flink.query1;

import com.diagiac.flink.RedisTSMapper;
import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

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
