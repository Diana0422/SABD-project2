package com.diagiac.flink.query1.bean;

import lombok.Data;

@Data
public class Query1Aggregator {
    private long count;
    private double temperatureSum;

    public Query1Aggregator(long aggCount, double aggSum) {
        this.count = aggCount;
        this.temperatureSum = aggSum;
    }
}
