package com.diagiac.flink.query3;

import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query3.bean.Query3Result;
import com.diagiac.flink.RedisHashSink;

public class RedisHashSink3 extends RedisHashSink<Query3Result> {

    private WindowEnum windowType;

    public RedisHashSink3(WindowEnum windowType) {
        this.windowType = windowType;
    }

    @Override
    public void setHashFieldsFrom(Query3Result flinkResult) {
        setHashField(flinkResult.getRedisKey(windowType), "timestamp", flinkResult.getTimestamp());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp0", flinkResult.getAvgTemp0());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp1", flinkResult.getAvgTemp1());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp2", flinkResult.getAvgTemp2());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp3", flinkResult.getAvgTemp3());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp4", flinkResult.getAvgTemp4());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp5", flinkResult.getAvgTemp5());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp6", flinkResult.getAvgTemp6());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp7", flinkResult.getAvgTemp7());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp8", flinkResult.getAvgTemp8());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp9", flinkResult.getAvgTemp9());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp10", flinkResult.getAvgTemp10());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp11", flinkResult.getAvgTemp11());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp12", flinkResult.getAvgTemp12());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp13", flinkResult.getAvgTemp13());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp14", flinkResult.getAvgTemp14());
        setHashField(flinkResult.getRedisKey(windowType), "avgTemp15", flinkResult.getAvgTemp15());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp0", flinkResult.getMedianTemp0());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp1", flinkResult.getMedianTemp1());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp2", flinkResult.getMedianTemp2());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp3", flinkResult.getMedianTemp3());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp4", flinkResult.getMedianTemp4());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp5", flinkResult.getMedianTemp5());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp6", flinkResult.getMedianTemp6());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp7", flinkResult.getMedianTemp7());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp8", flinkResult.getMedianTemp8());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp9", flinkResult.getMedianTemp9());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp10", flinkResult.getMedianTemp10());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp11", flinkResult.getMedianTemp11());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp12", flinkResult.getMedianTemp12());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp13", flinkResult.getMedianTemp13());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp14", flinkResult.getMedianTemp14());
        setHashField(flinkResult.getRedisKey(windowType), "medianTemp15", flinkResult.getMedianTemp15());
    }
}
