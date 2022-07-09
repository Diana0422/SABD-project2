package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.TemperatureMeasure;
import com.diagiac.flink.query2.bean.Query2Record;
import com.diagiac.flink.query2.bean.Query2Accumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregate2 implements AggregateFunction<Query2Record, Query2Accumulator, TemperatureMeasure> {
    public static final long serialVersionUID = 222141441412L;

    @Override
    public Query2Accumulator createAccumulator() {
        // creates initial accumulator
        return new Query2Accumulator(null, 0L, 0L, 0L, 0L);
    }

    @Override
    public Query2Accumulator add(Query2Record query2Record, Query2Accumulator query2Accumulator) {
        long aggCount = query2Record.getCount() + query2Accumulator.getCount();
        double aggTemp = query2Record.getTemperature() + query2Accumulator.getTemperatureSum();

//        if (query2Accumulator.getLocation() == 0L) {
//            query2Accumulator.setLocation(queryRecord2.getLocation());
//        }
//
//        if (query2Accumulator.getSensorId() == 0L){
//            query2Accumulator.setSensorId(queryRecord2.getSensor_id());
//        }

        return new Query2Accumulator(query2Record.getTimestamp(), aggCount, aggTemp,
                query2Record.getLocation(), query2Record.getSensor_id());
    }

    @Override
    public TemperatureMeasure getResult(Query2Accumulator accumulator) {
        return new TemperatureMeasure(accumulator.getTimestamp(), accumulator.getTemperatureSum() / accumulator.getCount(), accumulator.getSensorId());
    }

    @Override
    public Query2Accumulator merge(Query2Accumulator acc1, Query2Accumulator acc2) {
        return new Query2Accumulator(
                acc1.getTimestamp(),
                acc1.getCount() + acc2.getCount(),
                acc1.getTemperatureSum() + acc2.getTemperatureSum(),
                acc1.getLocation(),
                acc1.getSensorId()
        );
    }
}
