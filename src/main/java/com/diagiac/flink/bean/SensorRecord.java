package com.diagiac.flink.bean;

import lombok.Data;
import org.json.JSONObject;

import java.sql.Timestamp;

@Data
public class SensorRecord {
    private Long sensorId;
    private String sensorType;
    private Long location;
    private double lat;
    private double lon;
    private Timestamp timestamp;
    private String pressure;
    private String altitude;
    private String pressureSealevel;
    private double temperature;

    public static SensorRecord create(String rawMessage){
        JSONObject jsonObject = new JSONObject(rawMessage);
        SensorRecord record = new SensorRecord();
        record.setSensorId(Long.parseLong(jsonObject.getString("sensor_id")));
        record.setSensorType(jsonObject.getString("sensor_type"));
        record.setLocation(Long.parseLong(jsonObject.getString("location")));
        record.setLat(Double.parseDouble(jsonObject.getString("lat")));
        record.setLon(Double.parseDouble(jsonObject.getString("lon")));
        record.setTimestamp(Timestamp.valueOf(jsonObject.getString("timestamp").replace("T", " ")));
        record.setPressure(jsonObject.getString("pressure"));
        record.setAltitude(jsonObject.getString("altitude"));
        record.setPressureSealevel(jsonObject.getString("pressure_sealevel"));
        record.setTemperature(Double.parseDouble(jsonObject.getString("temperature")));
        return record;
    }
}
