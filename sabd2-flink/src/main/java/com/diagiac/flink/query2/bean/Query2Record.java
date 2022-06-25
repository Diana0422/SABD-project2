package com.diagiac.flink.query2.bean;

import com.diagiac.flink.SensorRecord;
import lombok.Data;

import java.sql.Timestamp;

@Data
public class Query2Record {
    private Long sensorId;
    private Timestamp timestamp;

    private Long location;
    private double temperature;
    private long count;

    public Query2Record(SensorRecord sensor){
        this.sensorId = sensor.getSensorId();
        this.location = sensor.getLocation();
        this.timestamp = sensor.getTimestamp();
        this.temperature = sensor.getTemperature();
        this.count = 1;
    }
}
