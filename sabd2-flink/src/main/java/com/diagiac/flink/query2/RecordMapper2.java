package com.diagiac.flink.query2;

import com.diagiac.flink.query2.bean.Query2Record;
import org.apache.flink.api.common.functions.MapFunction;

public class RecordMapper2 implements MapFunction<String, Query2Record> {
    @Override
    public Query2Record map(String valueRecord) {
        return Query2Record.create(valueRecord);
    }
}
