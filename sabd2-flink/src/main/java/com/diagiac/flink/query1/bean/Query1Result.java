package com.diagiac.flink.query1.bean;

import com.diagiac.flink.FlinkResult;
import com.diagiac.flink.WindowEnum;
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

    public String toStringCSV() {
        return timestamp.toString()+","+sensorId.toString()+","+count.toString()+","+avgTemperature.toString()+"\n";
    }

    @Override
    public String getKey(WindowEnum windowType) {
        return windowType.name() + ":" + sensorId.toString();

    }
}
