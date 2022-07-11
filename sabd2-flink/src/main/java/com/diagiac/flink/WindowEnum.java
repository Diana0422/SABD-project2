package com.diagiac.flink;

import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

/**
 * A simple enum that represents the possible windows for a flink Query.
 */
public enum WindowEnum {
    Hour, Day, Week, FromStart;


    private static final Map<WindowEnum, TumblingEventTimeWindows> mapWindow = Map.of(
            Hour, TumblingEventTimeWindows.of(Time.hours(1)),
            Day, TumblingEventTimeWindows.of(Time.days(1)),
            Week, TumblingEventTimeWindows.of(Time.days(7)),
            FromStart, TumblingEventTimeWindows.of(Time.days(30))
    );

    /**
     * This method is used to get a corresponding Tubling Event Time Window from the enum variant.
     *
     * @return a TumblingEventTimeWindows or an error
     */
    public TumblingEventTimeWindows getWindowStrategy() {
        return mapWindow.get(this);
    }
}
