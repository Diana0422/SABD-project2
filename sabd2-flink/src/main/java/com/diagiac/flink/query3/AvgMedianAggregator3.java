package com.diagiac.flink.query3;

import com.diagiac.flink.query3.bean.AvgMedian;
import com.diagiac.flink.query3.bean.Query3Aggregator;
import com.diagiac.flink.query3.bean.Query3Cell;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AvgMedianAggregator3 implements AggregateFunction<Query3Cell, Query3Aggregator, AvgMedian> {

    @Override
    public Query3Aggregator createAccumulator() {
        return null;
    }

    @Override
    public Query3Aggregator add(Query3Cell value, Query3Aggregator accumulator) {
        return null;
    }

    @Override
    public AvgMedian getResult(Query3Aggregator accumulator) {
        return null;
    }

    @Override
    public Query3Aggregator merge(Query3Aggregator a, Query3Aggregator b) {
        return null;
    }
}
