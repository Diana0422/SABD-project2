package com.diagiac.flink.redis;

import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class TrueRedisMapper1 implements RedisMapper<Query1Result> {

    private final String key; // List's key

    public TrueRedisMapper1(String key){
        this.key = key;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, key);
    }

    @Override
    public String getKeyFromData(Query1Result data) {
        return data.getSensorId().toString();
    }

    @Override
    public String getValueFromData(Query1Result data) {
        return data.toString();
    }
}