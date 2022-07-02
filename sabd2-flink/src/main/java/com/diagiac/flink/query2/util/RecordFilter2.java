package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.Query2Record;
import org.apache.flink.api.common.functions.FilterFunction;

public class RecordFilter2 implements FilterFunction<Query2Record> {
    private static final long serialVersionUID = 1291826918411L;

    @Override
    public boolean filter(Query2Record value) throws Exception {
        Double temperature = value.getTemperature();
        Long location = value.getLocation();
        boolean temperatureIsPresent = temperature != null;
        boolean locationIsPresent = location != null;
        if (temperatureIsPresent && locationIsPresent) {
            return temperature > -93.2 && temperature < 56.7;
        } else {
            return false;
        }
    }

}
