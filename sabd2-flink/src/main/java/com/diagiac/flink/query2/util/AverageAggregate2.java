package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.LocationTemperature;
import com.diagiac.flink.query2.bean.Query2Record;
import com.diagiac.flink.query2.bean.Query2Accumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregate2 implements AggregateFunction<Query2Record, Query2Accumulator, LocationTemperature> {
    public static final long serialVersionUID = 222141441412L;

    @Override
    public Query2Accumulator createAccumulator() {
        // creates initial accumulator
        return new Query2Accumulator(null, 0L, 0L, 0L);
    }

    @Override
    public Query2Accumulator add(Query2Record queryRecord2, Query2Accumulator query2Accumulator) {
        long aggCount = queryRecord2.getCount() + query2Accumulator.getCount();
        double aggTemp = queryRecord2.getTemperature() + query2Accumulator.getTemperatureSum();

        if (query2Accumulator.getLocation() == 0L) {
            query2Accumulator.setLocation(queryRecord2.getLocation());
        }

        return new Query2Accumulator(queryRecord2.getTimestamp(), aggCount, aggTemp, queryRecord2.getLocation());
    }

    @Override
    public LocationTemperature getResult(Query2Accumulator accumulator) {
        return new LocationTemperature(accumulator.getTimestamp(), accumulator.getTemperatureSum() / accumulator.getCount(), accumulator.getLocation());
    }

    @Override
    public Query2Accumulator merge(Query2Accumulator acc1, Query2Accumulator acc2) {
        return new Query2Accumulator(
                acc1.getTimestamp(),
                acc1.getCount() + acc2.getCount(),
                acc1.getTemperatureSum() + acc2.getTemperatureSum(),
                acc1.getLocation()
        );
    }
}
