package com.diagiac.flink.query1.utils;

import com.diagiac.flink.query1.bean.Query1Record;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filter for the query1 that filtes out all outlier temperatures
 * and all sensors with sensor_id >= 10000
 */
public class RecordFilter1 implements FilterFunction<Query1Record> {
    private static final long serialVersionUID = 1111111111111111L;
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
