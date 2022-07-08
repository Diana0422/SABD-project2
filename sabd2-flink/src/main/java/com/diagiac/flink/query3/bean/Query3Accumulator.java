package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.model.GeoCell;
import com.diagiac.flink.query3.util.P2MedianEstimator;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class Query3Accumulator {
    private Timestamp timestamp;
    private long count;
    private double temperatureSum;
    private GeoCell cell;
    private P2MedianEstimator medianEstimator;
    // private TreeSet<Query3Cell> orderedCellTemperatures;

    public Query3Accumulator(Timestamp timestamp, long count, double temperatureSum, GeoCell cell, P2MedianEstimator merged) {
        this.timestamp = timestamp;
        this.count = count;
        this.temperatureSum = temperatureSum;
        this.cell = cell;
//        this.orderedCellTemperatures = new TreeSet<>((o1, o2) -> {
//            int result = o1.getTemperature().compareTo(o2.getTemperature());
//            if (result == 0) {
//                return o1.getTimestamp().compareTo(o2.getTimestamp());
//            } else {
//                return result;
//            }
//        });
        this.medianEstimator = merged;
    }

    public void addData(Double temperature) {
//        orderedCellTemperatures.add(query3Cell);
        this.medianEstimator.add(temperature);
    }

    /**
     * Gets the median from the orderedCellTemperatures.
     *
     * @return the temperature of the central element or the mean of the two central elements.
     */
    public double calculateMedian() {
        return this.medianEstimator.getMedian();
//        if (orderedCellTemperatures.size() % 2 == 0) {
//            var central1 = Iterables.get(orderedCellTemperatures, orderedCellTemperatures.size() / 2);
//            var central2 = Iterables.get(orderedCellTemperatures, orderedCellTemperatures.size() / 2 - 1);
//            return (central1.getTemperature() + central2.getTemperature()) / 2.0;
//        } else {
//            return Iterables.get(orderedCellTemperatures, orderedCellTemperatures.size() / 2).getTemperature();
//        }
    }

    public double calculateAverage() {
        return temperatureSum / count;
    }
}
