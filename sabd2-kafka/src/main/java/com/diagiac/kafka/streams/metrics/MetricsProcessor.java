package com.diagiac.kafka.streams.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class MetricsProcessor implements Processor<Windowed<Long>, String, Void, Void> {
    private transient double throughput;
    private transient double latency;
    private transient long counter;
    private transient double start;
    private transient String windowType;

    public MetricsProcessor(String windowType) {
        this.windowType = windowType;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        System.out.println("OPEN METRICS");
        this.start = System.currentTimeMillis();
        this.throughput = 0;
        this.latency = 0;
        this.counter = 0;
        context.metrics().addLatencyRateTotalSensor("scope", "sabd-project", "avg-latency", Sensor.RecordingLevel.INFO);
        context.metrics().addRateTotalSensor("scope", "sabd-project", "avg-thoughput", Sensor.RecordingLevel.INFO);
        Processor.super.init(context);
    }

    @Override
    public void process(Record<Windowed<Long>, String> record) {
        System.out.println("PERFORMANCES: "+windowType);
        counter++;
        this.counter++;

        // gets milliseconds from the start of this operator
        double elapsed_millis = System.currentTimeMillis() - this.start;
        // gets seconds from milliseconds
        double elapsed_sec = elapsed_millis / 1000;

        // Compute throughput and latency
        this.throughput = this.counter / elapsed_sec; // tuple / s
        //this.throughput = this.counter / elapsed_millis; // tuple / ms
        this.latency = elapsed_millis / this.counter; // ms / tuple

        System.out.println("\tthroughput: "+throughput);
        System.out.println("\tlatency: "+latency);
    }

    @Override
    public void close() {
        this.latency = 0;
        this.throughput = 0;
        this.counter = 0;
        Processor.super.close();
    }
}
