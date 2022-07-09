package com.diagiac.flink.query2.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * Simple bean to represent timestamp, avgTemperature and sensor id of a sensor
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemperatureMeasure {
    private Timestamp timestamp;
    private Double avgTemperature;
    private Long sensorId;
}
