package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.Query2Result;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class RankingProcessWindowFunction extends ProcessWindowFunction<Query2Result, Query2Result, Timestamp, TimeWindow> {
    @Override
    public void process(Timestamp timestamp, ProcessWindowFunction<Query2Result, Query2Result, Timestamp, TimeWindow>.Context context, Iterable<Query2Result> iterable, Collector<Query2Result> collector) throws Exception {
        Timestamp ts = new Timestamp(context.window().getStart());
        for (Query2Result query2Result : iterable) {
            query2Result.setTimestamp(ts);
            collector.collect(query2Result);
        }
    }
}
