package com.diagiac.flink.query3;

import com.diagiac.flink.query3.bean.CellAvgMedianTemperature;
import com.diagiac.flink.query3.bean.Query3Aggregator;
import com.diagiac.flink.query3.bean.Query3Cell;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AvgMedianAggregator3 implements AggregateFunction<Query3Cell, Query3Aggregator, CellAvgMedianTemperature> {

    @Override
    public Query3Aggregator createAccumulator() {
        return new Query3Aggregator(0L, 0L, null);
    }

    @Override
    public Query3Aggregator add(Query3Cell query3Cell, Query3Aggregator query3Aggregator) {
        long aggCount = query3Cell.getCount() + query3Aggregator.getCount();
        double aggTemp = query3Cell.getTemperature() + query3Aggregator.getTemperatureSum();

        if (query3Aggregator.getCell() == null) {
            query3Aggregator.setCell(query3Cell.getCell());
        }

        return new Query3Aggregator(aggCount, aggTemp, query3Cell.getCell());
    }

    @Override
    public CellAvgMedianTemperature getResult(Query3Aggregator accumulator) {
        // Cell, avg temperature, median temperature
        return new CellAvgMedianTemperature(accumulator.getTemperatureSum() / accumulator.getCount(),0.0, accumulator.getCell());
    }

    @Override
    public Query3Aggregator merge(Query3Aggregator acc1, Query3Aggregator acc2) {
        return new Query3Aggregator(
                acc1.getCount() + acc2.getCount(),
                acc1.getTemperatureSum() + acc2.getTemperatureSum(),
                acc1.getCell()
        );
    }
}
