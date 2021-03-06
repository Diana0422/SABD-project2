package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.bean.CellAvgMedianTemperature;
import com.diagiac.flink.query3.bean.Query3Result;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

//public class FinalProcessWindowFunction extends ProcessAllWindowFunction<CellAvgMedianTemperature, Query3Result, TimeWindow> {
//    /**
//     * Calculates the average and the median temperature for each cell in this window.
//     * If a cell has no measurements, it will add an empty CellAvgMedianTemperature object to the result.
//     * @param context The context in which the window is being evaluated.
//     * @param elements The elements in the window being evaluated.
//     * @param out A collector for emitting elements.
//     * @throws Exception
//     */
//    @Override
//    public void process(ProcessAllWindowFunction<CellAvgMedianTemperature, Query3Result, TimeWindow>.Context context, Iterable<CellAvgMedianTemperature> elements, Collector<Query3Result> out) throws Exception {
//        CellAvgMedianTemperature[] orderedCells = new CellAvgMedianTemperature[16];
//        for (int i = 0; i < orderedCells.length; i++) {
//            orderedCells[i] = new CellAvgMedianTemperature(i, new Timestamp(context.window().getStart()));
//        }
//        for (CellAvgMedianTemperature element : elements) {
//            orderedCells[element.getCell().getId()] = element;
//        }
//        out.collect(new Query3Result(orderedCells));
//    }
//}

/**
 * Puts all 16 CellAvgMedianTemperature (even those with no measurements) in the same Query3Result,
 * to print everything in the requested order on the csv and show correctly the avg and medians on Grafana
 */
public class FinalProcessWindowFunction extends ProcessWindowFunction<CellAvgMedianTemperature, Query3Result, Timestamp, TimeWindow> {
    @Override
    public void process(Timestamp timestamp, ProcessWindowFunction<CellAvgMedianTemperature, Query3Result, Timestamp, TimeWindow>.Context context, Iterable<CellAvgMedianTemperature> elements, Collector<Query3Result> out) throws Exception {
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
