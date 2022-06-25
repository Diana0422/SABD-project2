package com.diagiac.flink.query1.utils;

import com.diagiac.flink.query1.bean.Query1Aggregator;
import com.diagiac.flink.query1.bean.Query1Record;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregator implements AggregateFunction <Query1Record, Query1Aggregator, Double> {
    @Override
    public Query1Aggregator createAccumulator() {
        return new Query1Aggregator(0L, 0L);
    }

    @Override
    public Query1Aggregator add(Query1Record queryRecord1, Query1Aggregator query1Aggregator) {
        long aggCount = queryRecord1.getCount() + query1Aggregator.getCount();
        double aggTemp = queryRecord1.getTemperature() + query1Aggregator.getTemperatureSum();
        Query1Aggregator ret = new Query1Aggregator(aggCount, aggTemp);
        return ret;
    }

    @Override
    public Double getResult(Query1Aggregator query1Aggregator) {
        return query1Aggregator.getTemperatureSum() / query1Aggregator.getCount();
    }

    @Override
    public Query1Aggregator merge(Query1Aggregator acc1, Query1Aggregator acc2) {
        return new Query1Aggregator(
                acc1.getCount()+acc2.getCount(),
                acc1.getTemperatureSum()+acc2.getTemperatureSum()
        );
    }
}
