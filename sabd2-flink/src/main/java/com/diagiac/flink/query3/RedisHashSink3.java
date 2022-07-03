package com.diagiac.flink.query3;

import com.diagiac.flink.query3.bean.Query3Result;
import com.diagiac.flink.RedisHashSink;

public class RedisHashSink3 extends RedisHashSink<Query3Result> {
    @Override
    public void setHashFieldsFrom(Query3Result flinkResult) {
        setHashField(flinkResult.getKey(), "timestamp", flinkResult.getTimestamp());
        setHashField(flinkResult.getKey(), "avgTemp0", flinkResult.getAvgTemp0());
        setHashField(flinkResult.getKey(), "avgTemp1", flinkResult.getAvgTemp1());
        setHashField(flinkResult.getKey(), "avgTemp2", flinkResult.getAvgTemp2());
        setHashField(flinkResult.getKey(), "avgTemp3", flinkResult.getAvgTemp3());
        setHashField(flinkResult.getKey(), "avgTemp4", flinkResult.getAvgTemp4());
        setHashField(flinkResult.getKey(), "avgTemp5", flinkResult.getAvgTemp5());
        setHashField(flinkResult.getKey(), "avgTemp6", flinkResult.getAvgTemp6());
        setHashField(flinkResult.getKey(), "avgTemp7", flinkResult.getAvgTemp7());
        setHashField(flinkResult.getKey(), "avgTemp8", flinkResult.getAvgTemp8());
        setHashField(flinkResult.getKey(), "avgTemp9", flinkResult.getAvgTemp9());
        setHashField(flinkResult.getKey(), "avgTemp10", flinkResult.getAvgTemp10());
        setHashField(flinkResult.getKey(), "avgTemp11", flinkResult.getAvgTemp11());
        setHashField(flinkResult.getKey(), "avgTemp12", flinkResult.getAvgTemp12());
        setHashField(flinkResult.getKey(), "avgTemp13", flinkResult.getAvgTemp13());
        setHashField(flinkResult.getKey(), "avgTemp14", flinkResult.getAvgTemp14());
        setHashField(flinkResult.getKey(), "avgTemp15", flinkResult.getAvgTemp15());
        setHashField(flinkResult.getKey(), "medianTemp0", flinkResult.getMedianTemp0());
        setHashField(flinkResult.getKey(), "medianTemp1", flinkResult.getMedianTemp1());
        setHashField(flinkResult.getKey(), "medianTemp2", flinkResult.getMedianTemp2());
        setHashField(flinkResult.getKey(), "medianTemp3", flinkResult.getMedianTemp3());
        setHashField(flinkResult.getKey(), "medianTemp4", flinkResult.getMedianTemp4());
        setHashField(flinkResult.getKey(), "medianTemp5", flinkResult.getMedianTemp5());
        setHashField(flinkResult.getKey(), "medianTemp6", flinkResult.getMedianTemp6());
        setHashField(flinkResult.getKey(), "medianTemp7", flinkResult.getMedianTemp7());
        setHashField(flinkResult.getKey(), "medianTemp8", flinkResult.getMedianTemp8());
        setHashField(flinkResult.getKey(), "medianTemp9", flinkResult.getMedianTemp9());
        setHashField(flinkResult.getKey(), "medianTemp10", flinkResult.getMedianTemp10());
        setHashField(flinkResult.getKey(), "medianTemp11", flinkResult.getMedianTemp11());
        setHashField(flinkResult.getKey(), "medianTemp12", flinkResult.getMedianTemp12());
        setHashField(flinkResult.getKey(), "medianTemp13", flinkResult.getMedianTemp13());
        setHashField(flinkResult.getKey(), "medianTemp14", flinkResult.getMedianTemp14());
        setHashField(flinkResult.getKey(), "medianTemp15", flinkResult.getMedianTemp15());
    }
}
