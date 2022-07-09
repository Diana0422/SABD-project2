package com.diagiac.flink.query1.bean;

import lombok.Data;

import java.sql.Timestamp;

/**
 * Aggregator bean used in the AverageAggregator class.
 */
@Data
public class Query1Aggregator {
    private Timestamp timestamp;
    private Long sensorId;
    private long count;
    private double temperatureSum;

    public Query1Aggregator(Timestamp timestamp, Long sensorId, long count, double temperatureSum) {
        this.timestamp = timestamp;
        this.sensorId = sensorId;
        this.count = count;
        this.temperatureSum = temperatureSum;
    }
}
