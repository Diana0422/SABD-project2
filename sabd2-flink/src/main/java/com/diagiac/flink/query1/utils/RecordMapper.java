package com.diagiac.flink.query1.utils;

import com.diagiac.flink.query1.bean.Query1Record;
import org.apache.flink.api.common.functions.MapFunction;

public class RecordMapper implements MapFunction<String, Query1Record> {
    @Override
    public Query1Record map(String valueRecord) {
        return Query1Record.create(valueRecord);
    }
}
