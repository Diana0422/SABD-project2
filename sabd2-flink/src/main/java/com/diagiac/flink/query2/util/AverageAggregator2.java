package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.Query2Record;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregator2 implements AggregateFunction<Query2Record, Query2Aggregator, LocationTemperature> {


    @Override
    public Query2Aggregator createAccumulator() {
        // creates initial accumulator
        return new Query2Aggregator(0L, 0L, 0L);
    }

    @Override
    public Query2Aggregator add(Query2Record queryRecord2, Query2Aggregator query2Aggregator) {
        long aggCount = queryRecord2.getCount() + query2Aggregator.getCount();
        double aggTemp = queryRecord2.getTemperature() + query2Aggregator.getTemperatureSum();

        if (query2Aggregator.getLocation() == 0L) {
            query2Aggregator.setLocation(queryRecord2.getLocation());
        }

        return new Query2Aggregator(aggCount, aggTemp, queryRecord2.getLocation());
    }

    @Override
    public LocationTemperature getResult(Query2Aggregator accumulator) {
        return new LocationTemperature(accumulator.getTemperatureSum() / accumulator.getCount(), accumulator.getLocation());
    }

    @Override
    public Query2Aggregator merge(Query2Aggregator acc1, Query2Aggregator acc2) {
        return new Query2Aggregator(
                acc1.getCount() + acc2.getCount(),
                acc1.getTemperatureSum() + acc2.getTemperatureSum(),
                acc1.getLocation()
        );
    }
}
