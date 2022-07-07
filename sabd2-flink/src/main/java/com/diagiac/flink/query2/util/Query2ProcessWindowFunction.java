package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.LocationTemperature;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

public class Query2ProcessWindowFunction extends ProcessWindowFunction<LocationTemperature, LocationTemperature, Long, TimeWindow> {
    /**
     * It simply updates the window timestamp at the start of the window in each locationTemperature object in the window
     * @param aLong The key for which this window is evaluated.
     * @param context The context in which the window is being evaluated.
     * @param iterable The elements in the window being evaluated.
     * @param collector A collector for emitting elements.
     * @throws Exception
     */
    @Override
    public void process(Long aLong, ProcessWindowFunction<LocationTemperature, LocationTemperature, Long, TimeWindow>.Context context, Iterable<LocationTemperature> iterable, Collector<LocationTemperature> collector) throws Exception {
        Timestamp ts = new Timestamp(context.window().getStart());
        for (LocationTemperature locationTemperature : iterable) {
            collector.collect(new LocationTemperature(ts, locationTemperature.getAvgTemperature(), locationTemperature.getSensorId()));
        }
        // iterable.iterator().forEachRemaining(locationTemperature -> collector.collect(new LocationTemperature(ts, locationTemperature.getAvgTemperature(), locationTemperature.getLocation())));
    }
}
