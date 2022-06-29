package com.diagiac.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class MetricRichMapFunction<T> extends RichMapFunction<T, T> {
    // transient keyword means that the field will NOT be serialized
    private transient double throughput = 0;
    private transient double latency = 0;
    private transient long counter = 0;
    private transient double start;

    @Override
    public void open(Configuration config) {
        System.out.println("!!SONO NELLA OPEN!!!");
        getRuntimeContext().getMetricGroup()
                .gauge("throughput", () -> this.throughput);

        getRuntimeContext().getMetricGroup()
                .gauge("latency", () -> this.latency);

        this.start = System.currentTimeMillis();

    }

    @Override
    public T map(T value) throws Exception {
        this.counter++;

        // Compute throughput and latency
        double elapsed_millis = System.currentTimeMillis() - this.start;
        double elapsed_sec = elapsed_millis / 1000;

        this.throughput = this.counter / elapsed_sec;
        this.latency = elapsed_millis / this.counter;

        return value;
    }
}
