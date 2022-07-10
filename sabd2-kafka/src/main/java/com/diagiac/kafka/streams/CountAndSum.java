package com.diagiac.kafka.streams;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class CountAndSum {
    private Timestamp ts;
    private long count;
    private double sum;

    public CountAndSum(Timestamp ts, long l, double v) {
        this.ts = ts;
        this.count = l;
        this.sum = v;
    }
}
