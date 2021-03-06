package com.diagiac.flink.query2;

import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query2.bean.Query2Result;
import com.diagiac.flink.RedisHashSink;

/**
 * This sink is used to save query2Results to a redis Hash set.
 * Each hashset only have 21 fields: timestamp, 10 min and max temperatures and 10 corresponding locations
 */
public class RedisHashSink2 extends RedisHashSink<Query2Result> {

    private final WindowEnum windowType;

    public RedisHashSink2(WindowEnum windowType) {
        this.windowType = windowType;
    }

    @Override
    public void setHashFieldsFrom(Query2Result flinkResult) {
        String key = flinkResult.getRedisKey(windowType);
        setHashField(key, "timestamp", flinkResult.getTimestamp());
        setHashField(key, "location1", flinkResult.getLocation1());// max temperature location
        setHashField(key, "location2", flinkResult.getLocation2());
        setHashField(key, "location3", flinkResult.getLocation3());
        setHashField(key, "location4", flinkResult.getLocation4());
        setHashField(key, "location5", flinkResult.getLocation5());
        setHashField(key, "location6", flinkResult.getLocation6()); // min temperature location
        setHashField(key, "location7", flinkResult.getLocation7());
        setHashField(key, "location8", flinkResult.getLocation8());
        setHashField(key, "location9", flinkResult.getLocation9());
        setHashField(key, "location10", flinkResult.getLocation10());
        setHashField(key, "temperature1", flinkResult.getTemperature1()); // max temperature
        setHashField(key, "temperature2", flinkResult.getTemperature2());
        setHashField(key, "temperature3", flinkResult.getTemperature3());
        setHashField(key, "temperature4", flinkResult.getTemperature4());
        setHashField(key, "temperature5", flinkResult.getTemperature5());
        setHashField(key, "temperature6", flinkResult.getTemperature6()); // min temperature
        setHashField(key, "temperature7", flinkResult.getTemperature7());
        setHashField(key, "temperature8", flinkResult.getTemperature8());
        setHashField(key, "temperature9", flinkResult.getTemperature9());
        setHashField(key, "temperature10", flinkResult.getTemperature10());
    }
}
