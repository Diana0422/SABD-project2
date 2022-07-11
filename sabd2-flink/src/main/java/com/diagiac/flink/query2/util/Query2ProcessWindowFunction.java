package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.TemperatureMeasure;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

public class Query2ProcessWindowFunction extends ProcessWindowFunction<TemperatureMeasure, TemperatureMeasure, Long, TimeWindow> {
    /**
     * It simply updates the window timestamp at the start of the window in each TemperatureMeasure object in the window
     * @param aLong The key for which this window is evaluated.
     * @param context The context in which the window is being evaluated.
     * @param iterable The elements in the window being evaluated.
     * @param collector A collector for emitting elements.
     * @throws Exception
     */
    @Override
    public void process(Long aLong, ProcessWindowFunction<TemperatureMeasure, TemperatureMeasure, Long, TimeWindow>.Context context, Iterable<TemperatureMeasure> iterable, Collector<TemperatureMeasure> collector) throws Exception {
        Timestamp ts = new Timestamp(context.window().getStart());
        for (TemperatureMeasure temperatureMeasure : iterable) {
            collector.collect(new TemperatureMeasure(ts, temperatureMeasure.getAvgTemperature(), temperatureMeasure.getSensorId()));
        }
    }
}
