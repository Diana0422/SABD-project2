package com.diagiac.flink.query1.utils;

import com.diagiac.flink.query1.bean.KeyQuery1;
import com.diagiac.flink.query1.bean.Query1Record;
import org.apache.flink.api.java.functions.KeySelector;

public class KeyExtractor implements KeySelector<Query1Record, KeyQuery1> {
    @Override
    public KeyQuery1 getKey(Query1Record queryRecord1) throws Exception {
        return KeyQuery1.create(queryRecord1.getTimestamp(), queryRecord1.getSensorId());
    }
}
