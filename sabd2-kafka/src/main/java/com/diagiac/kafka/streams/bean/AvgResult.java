package com.diagiac.kafka.streams.bean;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class AvgResult {
    private Timestamp ts;
    private long sensor;
    private long count;
    private double avgTemp;

    public AvgResult(long sensor, Timestamp ts, long count, double avgTemp) {
        this.sensor = sensor;
        this.avgTemp = avgTemp;
        this.ts = ts;
        this.count = count;
    }

    public String toStringCSV(){
        return ts.toString()+","
                +sensor+","
                +count+","
                +avgTemp+"\n";
    }
}
