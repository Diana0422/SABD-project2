package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.model.GeoCell;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class Query3Aggregator {
    private Timestamp timestamp;
    private long count;
    private double temperatureSum;
    private GeoCell cell;

    public Query3Aggregator(Timestamp timestamp, long count, double temperatureSum, GeoCell cell) {
        this.timestamp = timestamp;
        this.count = count;
        this.temperatureSum = temperatureSum;
        this.cell = cell;
    }
}
