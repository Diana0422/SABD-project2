package com.diagiac.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * This class is used to save Flink Result to Redis, but needs to be implemented for each query
 *
 * @param <T> The specific FlinkResult
 */
public abstract class RedisHashSink<T extends FlinkResult> extends RichSinkFunction<T> {

    private transient Jedis jedis; // must not be serialized.

    /**
     * Opens the Jedis connections
     *
     * @param config useless stuff
     */
    @Override
    public void open(Configuration config) {
        jedis = new Jedis("redis-cache", 6379);
    }

    /**
     * Called for every FlinkResult that comes from Flink
     * @param flinkResult The input record.
     * @param context Additional context about the input record.
     */
    @Override
    public void invoke(T flinkResult, Context context) {
        if (!jedis.isConnected()) {
            jedis.connect();
        }
        //Preservation
        setHashFieldsFrom(flinkResult);
    }

    /**
     * This method is used to save a FlinkResult inside a Redis Hash.
     * Call setHashField() for each field of the flinkResult to save into the Redis hash.
     *
     * @param flinkResult a single bean object coming from a Flink DataStream, that must be saved in Redis.
     */
    public abstract void setHashFieldsFrom(T flinkResult);


    public void setHashField(String key, String field, Object value) {
        jedis.hset(key, field, value.toString());
    }

    /**
     * Closes the jedis connection.
     *
     */
    @Override
    public void close() {
        jedis.close();
    }
}
