package com.diagiac.flink.redis;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
@Deprecated(since = "non va")
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
