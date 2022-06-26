package com.diagiac.flink.query1.utils;

import com.diagiac.flink.FlinkRecord;
import com.diagiac.flink.query1.bean.Query1Record;
import org.apache.flink.api.common.functions.FilterFunction;

public class RecordFilter1 implements FilterFunction<Query1Record> {
    @Override
    public boolean filter(Query1Record value) throws Exception {
        Double temperature = value.getTemperature();
        Long sensorId = value.getSensorId();
        boolean temperatureIsPresent = temperature != null;
        boolean sensorIsPresent = sensorId != null;
        if (temperatureIsPresent && sensorIsPresent) {
            boolean validTemperature = temperature > -93.2 && temperature < 56.7;
            boolean validSensor = sensorId < 10000;
            return validSensor && validTemperature;
        } else {
            return false;
        }
    }
}
