package com.diagiac.flink.query1;

import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisMapper1 implements RedisMapper<Query1Result> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    @Override
    public String getKeyFromData(Query1Result query1Result) {
        // sensor id is the KEY
        return query1Result.getSensorId().toString();
    }

    @Override
    public String getValueFromData(Query1Result query1Result) {
        // value contains avg of the temperature
        StringBuilder sb = new StringBuilder();
        String timestamp = query1Result.getTimestamp().toString();
        String avg = query1Result.getAvgTemperature().toString();
        sb.append(timestamp).append(" ").append(avg);
        return sb.toString();
    }
}
