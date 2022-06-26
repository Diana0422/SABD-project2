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

    public static Query1Record create(String rawMessage){
        JSONObject jsonObject = new JSONObject(rawMessage);
        Query1Record record = new Query1Record();
        String sensor = jsonObject.getString("sensor_id");
        String timestamp = jsonObject.getString("timestamp").replace("T", " ");
        String temperature = jsonObject.getString("temperature");

        if (sensor.isEmpty()) {
            record.setSensorId(null);
        } else {
            record.setSensorId(Long.parseLong(sensor));
        }
        if (timestamp.isEmpty()) {
            record.setTimestamp(null);
        } else {
            record.setTimestamp(Timestamp.valueOf(timestamp));
        }
        if (temperature.isEmpty()) {
            record.setTemperature(null);
        } else {
            record.setTemperature(Double.parseDouble(temperature));
        }
        record.setCount(1);
        return record;
    }
}
