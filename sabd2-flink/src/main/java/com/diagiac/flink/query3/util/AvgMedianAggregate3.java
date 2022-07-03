package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.bean.CellAvgMedianTemperature;
import com.diagiac.flink.query3.bean.Query3Accumulator;
import com.diagiac.flink.query3.bean.Query3Cell;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AvgMedianAggregate3 implements AggregateFunction<Query3Cell, Query3Accumulator, CellAvgMedianTemperature> {

    @Override
    public Query3Accumulator createAccumulator() {
        return new Query3Accumulator(null, 0L, 0L, null);
    }

    @Override
    public Query3Accumulator add(Query3Cell query3Cell, Query3Accumulator query3Accumulator) {
        // get count and sum of temperatures
        long aggCount = query3Cell.getCount() + query3Accumulator.getCount();
        double aggTemp = query3Cell.getTemperature() + query3Accumulator.getTemperatureSum();
        // if cell is null, add also the cell
        if (query3Accumulator.getCell() == null) {
            query3Accumulator.setCell(query3Cell.getCell());
        }

        // add the ordered set of Query3Cell (we only care about the temperature, we use the entire object because
        // if we have the same temperature, we lose duplicate elements in the treeSet)
        Query3Accumulator query3Accumulator1 = new Query3Accumulator(query3Cell.getTimestamp(), aggCount, aggTemp, query3Cell.getCell(), query3Accumulator.getOrderedCellTemperatures());
        query3Accumulator1.addData(query3Cell); // add the new query3Cell from the parameter
        return query3Accumulator1; // return the new and complete accumulator
    }

    @Override
    public CellAvgMedianTemperature getResult(Query3Accumulator accumulator) {
        // Cell, avg temperature, median temperature
        CellAvgMedianTemperature cellAvgMedianTemperature = new CellAvgMedianTemperature(
                accumulator.getTimestamp(),
                accumulator.calculateAverage(),
                accumulator.calculateMedian(),
                accumulator.getCell()
        );
        System.out.println("cellAvgMedianTemperature = " + cellAvgMedianTemperature);
        return cellAvgMedianTemperature;
    }

    @Override
    public Query3Accumulator merge(Query3Accumulator acc1, Query3Accumulator acc2) {
        // adds everything from acc2.getOrderedCellTemperatures() to acc1.getOrderedCellTemperatures()
        acc1.getOrderedCellTemperatures().addAll(acc2.getOrderedCellTemperatures());
        // add query3
        return new Query3Accumulator(
                acc1.getTimestamp(),
                acc1.getCount() + acc2.getCount(),
                acc1.getTemperatureSum() + acc2.getTemperatureSum(),
                acc1.getCell(),
                acc1.getOrderedCellTemperatures()
        );
    }

}
