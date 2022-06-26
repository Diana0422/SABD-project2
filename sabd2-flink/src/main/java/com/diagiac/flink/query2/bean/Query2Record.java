package com.diagiac.flink.query2.bean;

import com.diagiac.flink.FlinkRecord;
import com.diagiac.flink.SensorRecord;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.json.JSONObject;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
public class Query2Record implements FlinkRecord {
    private Timestamp timestamp;
    private Long location;
    private double temperature;
    private long count;

    public Query2Record(SensorRecord sensor){
        this.location = sensor.getLocation();
        this.timestamp = sensor.getTimestamp();
        this.temperature = sensor.getTemperature();
        this.count = 1;
    }

    public static Query2Record create(String valueRecord) {
        JSONObject jsonObject = new JSONObject(valueRecord);
        Query2Record record = new Query2Record();
        record.setLocation(Long.parseLong(jsonObject.getString("location")));
        record.setTimestamp(Timestamp.valueOf(jsonObject.getString("timestamp").replace("T", " ")));
        record.setTemperature(Double.parseDouble(jsonObject.getString("temperature")));
        record.setCount(1);
        return record;
    }
}
