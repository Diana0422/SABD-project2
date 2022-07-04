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

        // gets milliseconds from the start of this operator
        double elapsed_millis = System.currentTimeMillis() - this.start;
        // gets seconds from milliseconds
        double elapsed_sec = elapsed_millis / 1000;

        // Compute throughput and latency
        this.throughput = this.counter / elapsed_sec; // tuple / s
        this.latency = elapsed_millis / this.counter; // ms / tuple

        return value;
    }
}
