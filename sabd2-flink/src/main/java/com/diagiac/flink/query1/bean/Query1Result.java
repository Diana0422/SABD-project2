package com.diagiac.flink.query1.bean;

import com.diagiac.flink.FlinkResult;
import com.diagiac.flink.WindowEnum;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;

/**
 * The result bean for query1. Its key contains the sensor id.
 */
@Data
@AllArgsConstructor
public class Query1Result implements FlinkResult {
    private Timestamp timestamp;
    private Long sensorId;
    private Long count;
    private Double avgTemperature;

    public Query1Result(Query1Aggregator query1Aggregator) {
        this.timestamp = query1Aggregator.getTimestamp();
        this.sensorId = query1Aggregator.getSensorId();
        this.count = query1Aggregator.getCount();
        this.avgTemperature = query1Aggregator.getAverageTemperature();
    }

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
    public String getRedisKey(WindowEnum windowType) {
        return windowType.name() + ":" + sensorId.toString();
    }
}
