package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.bean.CellAvgMedianTemperature;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AvgMedianProcessWindow extends ProcessWindowFunction<CellAvgMedianTemperature, CellAvgMedianTemperature, Integer, TimeWindow> {
    @Override
    public void process(Integer integer, ProcessWindowFunction<CellAvgMedianTemperature, CellAvgMedianTemperature, Integer, TimeWindow>.Context context, Iterable<CellAvgMedianTemperature> elements, Collector<CellAvgMedianTemperature> out) throws Exception {
        Timestamp ts = new Timestamp(context.window().getStart());
        elements.iterator().forEachRemaining(cellAvgMedianTemperature -> out.collect(new CellAvgMedianTemperature(ts, cellAvgMedianTemperature.getAvgTemperature(), cellAvgMedianTemperature.getMedianTemperature(), cellAvgMedianTemperature.getCell())));
    }
}
