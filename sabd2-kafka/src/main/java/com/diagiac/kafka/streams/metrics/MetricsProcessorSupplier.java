package com.diagiac.kafka.streams.metrics;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;


public class MetricsProcessorSupplier implements ProcessorSupplier<Windowed<Long>, String, Void, Void> {

    private final String windowType;

    public MetricsProcessorSupplier(String windowType) {
        this.windowType = windowType;
    }

    @Override
    public Processor<Windowed<Long>, String, Void, Void> get() {
        return new MetricsProcessor(windowType);
    }
}
