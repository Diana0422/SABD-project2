package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.model.GeoCell;
import com.diagiac.flink.query3.util.P2MedianEstimator;
import lombok.Data;

import java.sql.Timestamp;

/**
 * Accumulator for query3 that computes:
 * - average (sum / count)
 * - approximate median with P2 algorithm
 * and also has the corresponding timestamp and cell.
 */
@Data
public class Query3Accumulator {
    private Timestamp timestamp;
    private long count;
    private double temperatureSum;
    private GeoCell cell;
    private P2MedianEstimator medianEstimator;

    public Query3Accumulator(Timestamp timestamp, long count, double temperatureSum, GeoCell cell, P2MedianEstimator merged) {
        this.timestamp = timestamp;
        this.count = count;
        this.temperatureSum = temperatureSum;
        this.cell = cell;
        this.medianEstimator = merged;
    }

    public void addData(Double temperature) {
        this.medianEstimator.add(temperature);
    }

    /**
     * Gets the median from the orderedCellTemperatures.
     *
     * @return the temperature of the central element or the mean of the two central elements.
     */
    public double calculateMedian() {
        return this.medianEstimator.getMedian();
    }

    public double calculateAverage() {
        return temperatureSum / count;
    }
}
