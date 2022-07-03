package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.LocationTemperature;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

public class Query2ProcessWindowFunction extends ProcessWindowFunction<LocationTemperature, LocationTemperature, Long, TimeWindow> {
    @Override
    public void process(Long aLong, ProcessWindowFunction<LocationTemperature, LocationTemperature, Long, TimeWindow>.Context context, Iterable<LocationTemperature> iterable, Collector<LocationTemperature> collector) throws Exception {
        Timestamp ts = new Timestamp(context.window().getStart());
        iterable.iterator().forEachRemaining(locationTemperature -> collector.collect(new LocationTemperature(ts, locationTemperature.getAvgTemperature(), locationTemperature.getLocation())));
    }
}
