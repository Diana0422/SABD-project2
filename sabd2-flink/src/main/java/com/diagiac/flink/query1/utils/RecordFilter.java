package com.diagiac.flink.query1.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.json.JSONObject;

public class RecordFilter implements FilterFunction<String> {
    @Override
    public boolean filter(String s) throws Exception {
        /* Must filter all records that have a valid temperature value and also sensor_id < 10000 as requested.*/
        double temperature;
        long sensorId;
        JSONObject jsonObject = new JSONObject(s);

        boolean temperatureIsPresent = jsonObject.has("temperature")
                && !jsonObject.getString("temperature").isEmpty();
        boolean sensorIsPresent = jsonObject.has("sensor_id") && !jsonObject.getString("sensor_id").isEmpty();
        if (temperatureIsPresent) {
            temperature = Double.parseDouble(jsonObject.getString("temperature"));
        } else {
            return false;
        }
        if (sensorIsPresent) {
            sensorId = Long.parseLong(jsonObject.getString("sensor_id"));
        } else {
            return false;
        }
        boolean validTemperature = temperature > -93.2 && temperature < 56.7;
        boolean validSensor = sensorId < 10000;
        return validSensor && validTemperature;
    }
}
