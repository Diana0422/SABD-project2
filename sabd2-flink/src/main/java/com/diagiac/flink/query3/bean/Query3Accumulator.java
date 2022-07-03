package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.model.GeoCell;
import lombok.Data;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;

import java.sql.Timestamp;
import java.util.TreeSet;

@Data
public class Query3Accumulator {
    private Timestamp timestamp;
    private long count;
    private double temperatureSum;
    private GeoCell cell;

    private TreeSet<Query3Cell> orderedCellTemperatures;

    public Query3Accumulator(Timestamp timestamp, long count, double temperatureSum, GeoCell cell) {
        this.timestamp = timestamp;
        this.count = count;
        this.temperatureSum = temperatureSum;
        this.cell = cell;
        this.orderedCellTemperatures = new TreeSet<>((o1, o2) -> {
            int result = o1.getTemperature().compareTo(o2.getTemperature());
            if (result == 0) { // TODO: può capitare ???
                return o1.getTimestamp().compareTo(o2.getTimestamp());
            } else {
                return result;
            }
        });
    }

    public Query3Accumulator(Timestamp t, long count, double temperatureSum, GeoCell cell, TreeSet<Query3Cell> treeSet) {
        this(t, count, temperatureSum, cell);
        this.orderedCellTemperatures = treeSet;
    }

    public void addData(Query3Cell query3Cell) {
        orderedCellTemperatures.add(query3Cell);
    }

    /**
     * Gets the median from the orderedCellTemperatures.
     *
     * @return the temperature of the central element or the mean of the two central elements.
     */
    public double calculateMedian() {
        if (orderedCellTemperatures.size() % 2 == 0) {
            var central1 = Iterables.get(orderedCellTemperatures, orderedCellTemperatures.size() / 2);
            var central2 = Iterables.get(orderedCellTemperatures, orderedCellTemperatures.size() / 2 - 1);
            return (central1.getTemperature() + central2.getTemperature()) / 2.0;
        } else {
            return Iterables.get(orderedCellTemperatures, orderedCellTemperatures.size() / 2).getTemperature();
        }
    }

    public double calculateAverage() {
        return temperatureSum / count;
    }
}
