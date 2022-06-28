package com.diagiac.flink.query3.bean;

import com.diagiac.flink.FlinkRecord;
import lombok.Data;
import org.json.JSONObject;

import java.sql.Timestamp;

@Data
public class Query3Record implements FlinkRecord {
    private Timestamp timestamp;
    private Double latitude;
    private Double longitude;
    private Double temperature;

    public static Query3Record create(String rawMessage) {
        JSONObject jsonObject = new JSONObject(rawMessage);
        var record = new Query3Record();
        String latitude = jsonObject.getString("lat");
        String longitude = jsonObject.getString("lon");
        String timestamp = jsonObject.getString("timestamp").replace("T", " ");
        String temperature = jsonObject.getString("temperature");

        record.setTimestamp(timestamp.isEmpty() ? null : Timestamp.valueOf(jsonObject.getString("timestamp").replace("T", " ")));
        record.setTemperature(temperature.isEmpty() ? null : Double.parseDouble(jsonObject.getString("temperature")));
        record.setLatitude(latitude.isEmpty() ? null : Double.parseDouble(jsonObject.getString("lat")));
        record.setLongitude(longitude.isEmpty() ? null : Double.parseDouble(jsonObject.getString("lon")));
        return record;
    }
}
