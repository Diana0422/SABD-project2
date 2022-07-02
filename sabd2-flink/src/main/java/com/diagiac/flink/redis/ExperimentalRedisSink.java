package com.diagiac.flink.redis;

import com.diagiac.flink.FlinkResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * This class is used to save Flink Result to Redis, but needs to be implemented for each query
 *
 * @param <T> The specific FlinkResult
 */
public abstract class ExperimentalRedisSink<T extends FlinkResult> extends RichSinkFunction<T> {

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

    @Override
    public void invoke(T flinkResult, Context context) throws Exception {
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


    public void setHashField(Object key, Object field, Object value) {
        jedis.hset(key.toString(), field.toString(), value.toString());
    }

    /**
     * Closes the jedis connection.
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        jedis.close();
    }
}
