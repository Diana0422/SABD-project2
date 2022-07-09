package com.diagiac.flink.query1;

import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MetricsSink implements SinkFunction<Query1Result> {

    private final String context;

    public MetricsSink(String context) {
        this.context = context;
    }

    @Override
    public void invoke(Query1Result value, Context context) throws Exception {
       // stats counter
        SyncronizedMetricsCounter.incrementCounter(this.context);
    }
}
