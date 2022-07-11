package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.Query2Result;
import com.diagiac.flink.query2.bean.TemperatureMeasure;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class RankWindowAllFunction extends ProcessAllWindowFunction<Query2Result, Query2Result, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<Query2Result, Query2Result, TimeWindow>.Context context, Iterable<Query2Result> elements, Collector<Query2Result> out) throws Exception {
        Timestamp ts = new Timestamp(context.window().getStart());
        elements.iterator().forEachRemaining(q2r -> out.collect(new Query2Result(ts,
                q2r.getLocation1(),
                q2r.getLocation2(),
                q2r.getLocation3(),
                q2r.getLocation4(),
                q2r.getLocation5(),
                q2r.getLocation6(),
                q2r.getLocation7(),
                q2r.getLocation8(),
                q2r.getLocation9(),
                q2r.getLocation10(),
                q2r.getTemperature1(),
                q2r.getTemperature2(),
                q2r.getTemperature3(),
                q2r.getTemperature4(),
                q2r.getTemperature5(),
                q2r.getTemperature6(),
                q2r.getTemperature7(),
                q2r.getTemperature8(),
                q2r.getTemperature9(),
                q2r.getTemperature10())));
    }
}
