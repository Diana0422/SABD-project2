package com.diagiac.flink;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.io.Serializable;

public interface RedisTSMapper<T> extends Function, Serializable {


    /**
     * Extracts key from data.
     *
     * @param data source data
     * @return key
     */
    String getKeyFromData(T data);

    /**
     * Extracts value from data.
     *
     * @param data source data
     * @return value
     */
    String getValueFromData(T data);

    /**
     * Extracts timestamp from data.
     *
     * @param data source data
     * @return value
     */
    String getTimestampFromData(T data);
}
