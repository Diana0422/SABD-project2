package com.diagiac.flink;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public enum WindowEnum {
    Hour, Day, Week, FromStart;

    public TumblingEventTimeWindows getWindowStrategy() {
        TumblingEventTimeWindows eventTime;
        switch (this) {
            case Hour:
                eventTime = TumblingEventTimeWindows.of(Time.hours(1));
                break;
            case Day:
                eventTime = TumblingEventTimeWindows.of(Time.days(1));
                break;
            case Week:
                eventTime = TumblingEventTimeWindows.of(Time.days(7));
                break;
            case FromStart:
                eventTime = TumblingEventTimeWindows.of(Time.days(9999));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
        return eventTime;
    }
}
