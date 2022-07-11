package com.diagiac.flink.query1.utils;
import com.diagiac.flink.query1.bean.Query1Result;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * This simple processWindow Function is only used to
 * change the timestamp from the one from the tuple
 * to the timestamp at the start of the window.
 */
public class Query1ProcessWindowFunction extends ProcessWindowFunction<Query1Result, Query1Result, Long, TimeWindow> {
    @Override
    public void process(Long aLong, ProcessWindowFunction<Query1Result, Query1Result, Long, TimeWindow>.Context context, Iterable<Query1Result> iterable, Collector<Query1Result> collector) throws Exception {
        Timestamp ts = new Timestamp(context.window().getStart());
        System.out.println(ts);
        iterable.iterator().forEachRemaining(query1Result -> collector.collect(new Query1Result(ts, query1Result.getSensorId(), query1Result.getCount(), query1Result.getAvgTemperature())));
    }
}
