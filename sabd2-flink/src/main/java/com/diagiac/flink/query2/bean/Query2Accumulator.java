package com.diagiac.flink.query2.bean;

import lombok.Data;
import java.sql.Timestamp;

@Data
public class Query2Accumulator {

    private Timestamp timestamp;
    private Long location;

    private Long sensorId;
    private long count;
    private double temperatureSum;

    public Query2Accumulator(Timestamp timestamp, long aggCount, double aggSum, Long location, Long sensorId) {
        this.timestamp = timestamp;
        this.count = aggCount;
        this.temperatureSum = aggSum;
        this.location = location;
        this.sensorId = sensorId;
    }
}
