package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.bean.CellAvgMedianTemperature;
import com.diagiac.flink.query3.bean.Query3Result;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class FinalProcessWindowFunction extends ProcessAllWindowFunction<CellAvgMedianTemperature, Query3Result, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<CellAvgMedianTemperature, Query3Result, TimeWindow>.Context context, Iterable<CellAvgMedianTemperature> elements, Collector<Query3Result> out) throws Exception {
        CellAvgMedianTemperature[] orderedCells = new CellAvgMedianTemperature[16];
        for (int i = 0; i < orderedCells.length; i++) {
            orderedCells[i] = new CellAvgMedianTemperature(i, new Timestamp(context.window().getStart()));
        }
        for (CellAvgMedianTemperature element : elements) {
            orderedCells[element.getCell().getId()] = element;
        }
        out.collect(new Query3Result(orderedCells));
    }
}
