package com.diagiac.flink.query2.bean;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Query2Aggregator {

    private Timestamp timestamp;
    private Long location;
    private long count;
    private double temperatureSum;

    public Query2Aggregator(long aggCount, double aggSum, Long location) {
        this.count = aggCount;
        this.temperatureSum = aggSum;
        this.location = location;
    }
}
