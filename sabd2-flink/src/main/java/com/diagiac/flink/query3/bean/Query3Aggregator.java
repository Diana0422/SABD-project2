package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.util.GeoCell;
import lombok.Data;

@Data
public class Query3Aggregator {
    private long count;
    private double temperatureSum;
    private GeoCell cell;

    public Query3Aggregator(long count, double temperatureSum, GeoCell cell) {
        this.count = count;
        this.temperatureSum = temperatureSum;
        this.cell = cell;
    }
}
