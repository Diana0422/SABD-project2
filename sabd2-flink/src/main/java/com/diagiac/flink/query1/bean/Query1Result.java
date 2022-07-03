package com.diagiac.flink.query1.bean;

import com.diagiac.flink.FlinkResult;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
public class Query1Result implements FlinkResult {
    private Timestamp timestamp;
    private Long sensorId;
    private Long count;
    private Double avgTemperature;

    @Override
    public String toString() {
        return "Query1Result{" +
                "timestamp=" + timestamp +
                ", sensorId=" + sensorId +
                ", count=" + count +
                ", avgTemperature=" + avgTemperature +
                '}';
    }

    @Override
    public String getKey() {
        return this.sensorId.toString();
    }
}
