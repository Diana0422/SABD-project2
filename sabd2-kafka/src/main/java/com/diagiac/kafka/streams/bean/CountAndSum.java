package com.diagiac.kafka.streams.bean;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
public class CountAndSum {
//    private Timestamp ts;
    private long count;
    private double sum;

    public CountAndSum(long l, double v) {
//        this.ts = ts;
        this.count = l;
        this.sum = v;
    }
}
