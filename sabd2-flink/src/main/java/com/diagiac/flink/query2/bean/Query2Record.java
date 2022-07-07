package com.diagiac.flink.query2.bean;

import com.diagiac.flink.FlinkRecord;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.json.JSONObject;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
public class Query2Record implements FlinkRecord {
    private Timestamp timestamp;
    private Long location;
    private Long sensor_id;
    private Double temperature;
    private long count;


    public static Query2Record create(String jsonRecord) {
        JSONObject jsonObject = new JSONObject(jsonRecord);
        Query2Record query2Record = new Query2Record();
        String location = jsonObject.getString("location");
        String timestamp = jsonObject.getString("timestamp").replace("T", " ");
        String temperature = jsonObject.getString("temperature");
        String sensorId = jsonObject.getString("sensor_id");
        if (location.isEmpty()) {
            query2Record.setLocation(null);
        } else {
            query2Record.setLocation(Long.parseLong(location));
        }
        if (timestamp.isEmpty()) {
            query2Record.setTimestamp(null);
        } else {
            query2Record.setTimestamp(Timestamp.valueOf(timestamp));
        }
        if (temperature.isEmpty()) {
            query2Record.setTemperature(null);
        } else {
            query2Record.setTemperature(Double.parseDouble(temperature));
        }
        if (sensorId.isEmpty()){
            query2Record.setSensor_id(null);
        } else {
            query2Record.setSensor_id(Long.parseLong(sensorId));
        }
        query2Record.setCount(1);
        return query2Record;
    }
}
