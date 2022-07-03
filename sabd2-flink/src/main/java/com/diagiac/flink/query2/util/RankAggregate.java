package com.diagiac.flink.query2.util;
import com.diagiac.flink.query2.bean.LocationTemperature;
import com.diagiac.flink.query2.bean.Query2Result;
import com.diagiac.flink.query2.bean.RankAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

public class RankAggregate implements AggregateFunction<LocationTemperature, RankAccumulator, Query2Result> {
    @Override
    public RankAccumulator createAccumulator() {
        return new RankAccumulator();
    }

    @Override
    public RankAccumulator add(LocationTemperature locationTemperature, RankAccumulator rankAccumulator) {
        rankAccumulator.addData(locationTemperature);
        return rankAccumulator;
    }

    @Override
    public Query2Result getResult(RankAccumulator rankAccumulator) {
        return rankAccumulator.getResult();
    }

    @Override
    public RankAccumulator merge(RankAccumulator rankAccumulator, RankAccumulator acc1) {
        return null;
    }
}
