package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.bean.Query3Result;
import com.diagiac.flink.query3.bean.CellAvgMedianTemperature;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FinalProcessWindowFunction extends ProcessAllWindowFunction<CellAvgMedianTemperature, Query3Result, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<CellAvgMedianTemperature, Query3Result, TimeWindow>.Context context, Iterable<CellAvgMedianTemperature> elements, Collector<Query3Result> out) throws Exception {
        CellAvgMedianTemperature[] orderedCells = new CellAvgMedianTemperature[16];
        for (int i = 0; i < orderedCells.length; i++) {
            orderedCells[i] = new CellAvgMedianTemperature();
        }
        for (CellAvgMedianTemperature element : elements) {
            orderedCells[element.getCell().getId()] = element;
        }
        out.collect(new Query3Result(orderedCells));
    }
}
