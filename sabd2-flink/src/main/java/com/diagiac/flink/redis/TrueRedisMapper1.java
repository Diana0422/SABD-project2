package com.diagiac.flink.redis;

import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.lang.reflect.InvocationTargetException;

public class TrueRedisMapper1 implements RedisMapper<Query1Result> {

    private final String key; // HSET's key
    private final String field; // HSET's field
    private final String getter; // getter of the object to get HSET's value

    public  TrueRedisMapper1(String key, String field, String getter) {
        this.key = key;
        this.field = field;
        this.getter = getter;
    }

    /**
     * Sets the hash key for HSET. If does not exists, overwrites it.
     *
     * @return
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, key);
    }

    /**
     * Sets the hash field for HSET
     *
     * @param data source data
     * @return
     */
    @Override
    public String getKeyFromData(Query1Result data) {
        return field;
    }

    /**
     * Sets the hash value for HSET's field
     *
     * @param data source data
     * @return
     */
    @Override
    public String getValueFromData(Query1Result data) {
        try {
            return data.getClass().getMethod(getter).invoke(data).toString();
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            return "NO DATA";
        }
    }
}