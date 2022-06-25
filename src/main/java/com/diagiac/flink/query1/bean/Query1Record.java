package com.diagiac.flink.query1.bean;

import lombok.Data;
import org.json.JSONObject;

import java.sql.Timestamp;

@Data
public class Query1Record {
    private Long sensorId;
    private Timestamp timestamp;
    private double temperature;
    private long count;

    public static Query1Record create(String rawMessage){
        JSONObject jsonObject = new JSONObject(rawMessage);
        Query1Record record = new Query1Record();
        record.setSensorId(Long.parseLong(jsonObject.getString("sensor_id")));
        record.setTimestamp(Timestamp.valueOf(jsonObject.getString("timestamp").replace("T", " ")));
        record.setTemperature(Double.parseDouble(jsonObject.getString("temperature")));
        record.setCount(1);
        return record;
    }
}
