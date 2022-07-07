package com.diagiac.flink.query2.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LocationTemperature {
    private Timestamp timestamp;
    private Double avgTemperature;
    private Long sensorId;
}
