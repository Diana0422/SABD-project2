package com.diagiac.flink.query1.bean;

import com.diagiac.flink.FlinkRecord;
import lombok.Data;
import org.json.JSONObject;

import java.sql.Timestamp;

@Data
public class Query1Record  implements FlinkRecord {
    private Long sensorId;
    private Timestamp timestamp;
    private Double temperature;
    private long count;

    public static Query1Record create(String jsonRecord){
        JSONObject jsonObject = new JSONObject(jsonRecord);
        Query1Record query1Record = new Query1Record();
        String sensor = jsonObject.getString("sensor_id");
        String timestamp = jsonObject.getString("timestamp").replace("T", " ");
        String temperature = jsonObject.getString("temperature");

        if (sensor.isEmpty()) {
            query1Record.setSensorId(null);
        } else {
            query1Record.setSensorId(Long.parseLong(sensor));
        }
        if (timestamp.isEmpty()) {
            query1Record.setTimestamp(null);
        } else {
            query1Record.setTimestamp(Timestamp.valueOf(timestamp));
        }
        if (temperature.isEmpty()) {
            query1Record.setTemperature(null);
        } else {
            query1Record.setTemperature(Double.parseDouble(temperature));
        }
        query1Record.setCount(1);
        return query1Record;
    }
}
