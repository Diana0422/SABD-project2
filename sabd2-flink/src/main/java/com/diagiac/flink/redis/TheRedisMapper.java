package com.diagiac.flink.redis;

import com.diagiac.flink.FlinkResult;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.lang.reflect.InvocationTargetException;

@Deprecated(since = "Estendi ExperimentalRedisSink invece di usare questa classe N volte")
public class TheRedisMapper<T extends FlinkResult> implements RedisMapper<T> {

    private final String key; // HSET's key
    private final String field; // HSET's field
    private final String getter; // getter of the object to get HSET's value

    public TheRedisMapper(String key, String field, String getter) {
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
    public String getKeyFromData(T data) {
        return field;
    }

    /**
     * Sets the hash value for HSET's field
     *
     * @param data source data
     * @return
     */
    @Override
    public String getValueFromData(T data) {
        try {
            return data.getClass().getMethod(getter).invoke(data).toString();
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            return "NO DATA";
        }
    }
}