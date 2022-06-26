package com.diagiac.flink.query2.util;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * LocationTemperature : location + temperature
 * Query2Result: top5 min and max temperature + locations
 * TimeWindow: A window that represent a temporal interval
 */
public class SortKeyedProcessFunction extends ProcessAllWindowFunction<LocationTemperature, Query2Result, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<LocationTemperature, Query2Result, TimeWindow>.Context context,
                        Iterable<LocationTemperature> elements,
                        Collector<Query2Result> out) throws Exception {
        var list = new ArrayList<LocationTemperature>();
        for (LocationTemperature element : elements) {
            list.add(element);
        }

        list.sort(Comparator.comparing(LocationTemperature::getAvgTemperature));

        var size = list.size();

        // MaX temperatures
        List<LocationTemperature> maxTemperatures = list.subList(size - 5, size);
        List<LocationTemperature> minTemperatures = list.subList(0, 5);
        out.collect(new Query2Result(maxTemperatures, minTemperatures));
    }
}
