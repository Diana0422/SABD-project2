package com.diagiac.flink;

public interface FlinkResult {
    /**
     * @return the key for redis hash
     */
    String getKey();
}
