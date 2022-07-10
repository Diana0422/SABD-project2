package com.diagiac.flink.query1;

import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Deprecated
public class MetricsSink implements SinkFunction<Query1Result> {

    private final String context;
    private final SyncronizedMetricsCounter metricsCounter;
    public MetricsSink(String context) {
        this.context = context;
        this.metricsCounter = new SyncronizedMetricsCounter();
    }

    @Override
    public void invoke(Query1Result value, Context context) throws Exception {
       // stats counter
        metricsCounter.incrementCounter(this.context);
    }
}
