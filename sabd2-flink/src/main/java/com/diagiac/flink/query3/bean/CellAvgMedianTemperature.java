package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.model.GeoCell;
import lombok.Data;

import java.sql.Timestamp;

/**
 * Bean for the query3 to save intermediate results (avg temperature, median temperature, cell and timestamp)
 */
@Data
public class CellAvgMedianTemperature {
    private Timestamp timestamp;
    private double avgTemperature;
    private double medianTemperature;
    private GeoCell cell;

    public CellAvgMedianTemperature(int i, Timestamp timestamp){
        this.timestamp = timestamp;
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
