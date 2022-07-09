package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.TemperatureMeasure;
import com.diagiac.flink.query2.bean.Query2Result;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * LocationTemperature : location + temperature
 * Query2Result: top5 min and max temperature + locations
 * TimeWindow: A window that represent a temporal interval
 */
public class SortKeyedProcessFunction extends ProcessAllWindowFunction<TemperatureMeasure, Query2Result, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<TemperatureMeasure, Query2Result, TimeWindow>.Context context,
                        Iterable<TemperatureMeasure> elements,
                        Collector<Query2Result> out) throws Exception {
        var list = new ArrayList<TemperatureMeasure>();
        for (TemperatureMeasure element : elements) {
            list.add(element);
        }

        list.sort(Comparator.comparing(TemperatureMeasure::getAvgTemperature));

        var size = list.size();

        // MaX temperatures
        List<TemperatureMeasure> maxTemperatures = list.subList(size - 5, size);
        List<TemperatureMeasure> minTemperatures = list.subList(0, 5);
//        System.out.println("minTemperatures = " + minTemperatures.stream().map(LocationTemperature::getTimestamp).collect(Collectors.toList()));
        out.collect(new Query2Result(new Timestamp(context.window().getStart()), maxTemperatures, minTemperatures));
    }
}
