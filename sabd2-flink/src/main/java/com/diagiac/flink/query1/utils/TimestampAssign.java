package com.diagiac.flink.query1.utils;

import com.diagiac.flink.query1.bean.Query1Record;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

public class TimestampAssign implements TimestampAssigner<Query1Record> {

    @Override
    public long extractTimestamp(Query1Record queryRecord1, long l) {
        return 0;
    }
}
