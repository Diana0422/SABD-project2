package com.diagiac.flink.query1.utils;

import com.diagiac.flink.query1.bean.Query1Aggregator;
import com.diagiac.flink.query1.bean.Query1Record;
import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregator that computes mean of temperature for each individual sensor id less than 10000 (which is the key)
 */
public class AverageAggregate1 implements AggregateFunction<Query1Record, Query1Aggregator, Query1Result> {
    private static final long serialVersionUID = 33333333333333L;

    @Override
    public Query1Aggregator createAccumulator() {
        return new Query1Aggregator(null, 0L, 0L, 0.0);
    }

    @Override
    public Query1Aggregator add(Query1Record queryRecord1, Query1Aggregator query1Aggregator) {
        long aggCount = queryRecord1.getCount() + query1Aggregator.getCount();
        double aggTemp = queryRecord1.getTemperature() + query1Aggregator.getTemperatureSum();
        return new Query1Aggregator(queryRecord1.getTimestamp(), queryRecord1.getSensorId(), aggCount, aggTemp);
    }

    @Override
    public Query1Result getResult(Query1Aggregator query1Aggregator) {
        return new Query1Result(query1Aggregator);
    }

    @Override
    public Query1Aggregator merge(Query1Aggregator acc1, Query1Aggregator acc2) {
        return new Query1Aggregator(
                acc1.getTimestamp(),
                acc1.getSensorId(),
                acc1.getCount() + acc2.getCount(),
                acc1.getTemperatureSum() + acc2.getTemperatureSum()
        );
    }
}
