package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.bean.CellAvgMedianTemperature;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * It simply updates the window timestamp at the start of the window in each CellAvgMedianTemperature object in the window
 * @param aLong The key for which this window is evaluated.
 * @param context The context in which the window is being evaluated.
 * @param iterable The elements in the window being evaluated.
 * @param collector A collector for emitting elements.
 * @throws Exception
 */
public class AvgMedianProcessWindow extends ProcessWindowFunction<CellAvgMedianTemperature, CellAvgMedianTemperature, Integer, TimeWindow> {
    @Override
    public void process(Integer integer, ProcessWindowFunction<CellAvgMedianTemperature, CellAvgMedianTemperature, Integer, TimeWindow>.Context context, Iterable<CellAvgMedianTemperature> elements, Collector<CellAvgMedianTemperature> out) throws Exception {
        Timestamp ts = new Timestamp(context.window().getStart());
        elements.iterator().forEachRemaining(cellAvgMedianTemperature -> out.collect(new CellAvgMedianTemperature(ts, cellAvgMedianTemperature.getAvgTemperature(), cellAvgMedianTemperature.getMedianTemperature(), cellAvgMedianTemperature.getCell())));
    }
}
