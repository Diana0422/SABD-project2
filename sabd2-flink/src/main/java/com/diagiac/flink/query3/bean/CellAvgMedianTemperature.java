package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.util.GeoCell;
import lombok.Data;

@Data
public class CellAvgMedianTemperature {
    private double avgTemperature;
    private double medianTemperature;
    private GeoCell cell;

    public CellAvgMedianTemperature(){
        this.avgTemperature = Double.NaN;
        this.medianTemperature = Double.NaN;
        this.cell = null;
    }

    public CellAvgMedianTemperature(double avgTemperature, double medianTemperature, GeoCell cell) {
        this.avgTemperature = avgTemperature;
        this.medianTemperature = medianTemperature;
        this.cell = cell;
    }

    @Override
    public String toString() {
        return "CellAvgMedianTemperature{" +
               "avgTemperature=" + avgTemperature +
               ", medianTemperature=" + medianTemperature +
               ", cell=" + cell +
               '}';
    }
}
