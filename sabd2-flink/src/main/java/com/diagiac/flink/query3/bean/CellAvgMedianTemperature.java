package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.model.GeoCell;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class CellAvgMedianTemperature {
    private Timestamp timestamp;
    private double avgTemperature;
    private double medianTemperature;
    private GeoCell cell;

    public CellAvgMedianTemperature(int i){
        this.timestamp = null;
        this.avgTemperature = Double.NaN;
        this.medianTemperature = Double.NaN;
        this.cell = new GeoCell(i);
    }

    public CellAvgMedianTemperature(Timestamp timestamp, double avgTemperature, double medianTemperature, GeoCell cell) {
        this.timestamp = timestamp;
        this.avgTemperature = avgTemperature;
        this.medianTemperature = medianTemperature;
        this.cell = cell;
    }

    @Override
    public String toString() {
        return "CellAvgMedianTemperature{" +
                "timestamp=" + timestamp +
                ", avgTemperature=" + avgTemperature +
                ", medianTemperature=" + medianTemperature +
                ", cell=" + cell +
                '}';
    }
}
