package com.diagiac.flink.query2.util;

import com.diagiac.flink.query2.bean.Query2Record;
import org.apache.flink.api.common.functions.FilterFunction;
import org.json.JSONObject;

public class RecordFilter2 implements FilterFunction<Query2Record> {
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
    // Timestamp, Location, Temperature Not NULL
//    @Override
//    public boolean filter(String s) throws Exception {
//        /* Must filter all records that have a valid temperature value and also sensor_id < 10000 as requested.*/
//        double temperature;
//        JSONObject jsonObject = new JSONObject(s);
//
//        boolean temperatureIsPresent = jsonObject.has("temperature")
//                                       && !jsonObject.getString("temperature").isEmpty();
//
//        boolean locationIsPresent = jsonObject.has("location") && !jsonObject.getString("location").isEmpty();
//
//        if (temperatureIsPresent) {
//            temperature = Double.parseDouble(jsonObject.getString("temperature"));
//        } else {
//            return false; // TODO decidere se gestire la tupla non valida
//        }
//
//        boolean validTemperature = temperature > -93.2 && temperature < 56.7;
//        return validTemperature && locationIsPresent;
//    }

}
