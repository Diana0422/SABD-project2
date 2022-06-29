package com.diagiac.flink.query1.utils;

import com.diagiac.flink.query1.bean.Query1Aggregator;
import com.diagiac.flink.query1.bean.Query1Record;
import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregator implements AggregateFunction <Query1Record, Query1Aggregator, Query1Result> {
    @Override
    public Query1Aggregator createAccumulator() {
        return new Query1Aggregator(null, 0L, 0L, 0.0);
    }

    @Override
    public Query1Aggregator add(Query1Record queryRecord1, Query1Aggregator query1Aggregator) {
//        System.out.println("queryRecord1TS = " + queryRecord1.getTimestamp() + ", query1AggregatorTS = " + query1Aggregator.getTimestamp());
//        System.out.println("queryRecord1SENS = " + queryRecord1.getSensorId() + ", query1AggregatorSENS = " + query1Aggregator.getSensorId());
        long aggCount = queryRecord1.getCount() + query1Aggregator.getCount();
        double aggTemp = queryRecord1.getTemperature() + query1Aggregator.getTemperatureSum();
        return new Query1Aggregator(queryRecord1.getTimestamp(), queryRecord1.getSensorId(), aggCount, aggTemp);
    }

    @Override
    public Query1Result getResult(Query1Aggregator query1Aggregator) {
        return new Query1Result(query1Aggregator.getTimestamp(), query1Aggregator.getSensorId(), query1Aggregator.getCount(), query1Aggregator.getTemperatureSum() / query1Aggregator.getCount());
    }

    @Override
    public Query1Aggregator merge(Query1Aggregator acc1, Query1Aggregator acc2) {
        System.out.println("Timestamp1 = " + acc1.getTimestamp() + ", Timestamp2 = " + acc2.getTimestamp());
        System.out.println(" Sensor1= " + acc1.getSensorId() + ", Sensor2 = " + acc2.getSensorId());
        return new Query1Aggregator(
                acc1.getTimestamp(),
                acc1.getSensorId(),
                acc1.getCount()+acc2.getCount(),
                acc1.getTemperatureSum()+acc2.getTemperatureSum()
        );
    }
}
