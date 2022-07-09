package com.diagiac.flink;

/**
 * Marker interface to mark all Query#Results from a Flink query.
 * Has a method to implement that needs to return a string representing a Redis key
 * to save the result on a Redis Hash.
 */
public interface FlinkResult {
    /**
     * @return the key for redis hash
     */
    String getRedisKey(WindowEnum windowType);
}
