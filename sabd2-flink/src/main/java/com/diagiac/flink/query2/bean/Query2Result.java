package com.diagiac.flink.query2.bean;

import com.diagiac.flink.FlinkResult;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;
import java.util.List;

@Data
@AllArgsConstructor
public class Query2Result implements FlinkResult {
    private Timestamp timestamp;
    private Long location1;
    private Long location2;
    private Long location3;
    private Long location4;
    private Long location5;
    private Long location6;
    private Long location7;
    private Long location8;
    private Long location9;
    private Long location10;

    private Double temperature1;
    private Double temperature2;
    private Double temperature3;
    private Double temperature4;
    private Double temperature5;
    private Double temperature6;
    private Double temperature7;
    private Double temperature8;
    private Double temperature9;
    private Double temperature10;

    public Query2Result(Timestamp timestamp, List<LocationTemperature> maxTemperatures, List<LocationTemperature> minTemperatures) {
        this.timestamp = timestamp;
        location1 = maxTemperatures.get(4).getLocation();
        location2 = maxTemperatures.get(3).getLocation();
        location3 = maxTemperatures.get(2).getLocation();
        location4 = maxTemperatures.get(1).getLocation();
        location5 = maxTemperatures.get(0).getLocation();

        location6 = minTemperatures.get(0).getLocation();
        location7 = minTemperatures.get(1).getLocation();
        location8 = minTemperatures.get(2).getLocation();
        location9 = minTemperatures.get(3).getLocation();
        location10 = minTemperatures.get(4).getLocation();

        temperature1 = maxTemperatures.get(4).getAvgTemperature();
        temperature2 = maxTemperatures.get(3).getAvgTemperature();
        temperature3 = maxTemperatures.get(2).getAvgTemperature();
        temperature4 = maxTemperatures.get(1).getAvgTemperature();
        temperature5 = maxTemperatures.get(0).getAvgTemperature();

        temperature6 = minTemperatures.get(0).getAvgTemperature();
        temperature7 = minTemperatures.get(1).getAvgTemperature();
        temperature8 = minTemperatures.get(2).getAvgTemperature();
        temperature9 = minTemperatures.get(3).getAvgTemperature();
        temperature10 = minTemperatures.get(4).getAvgTemperature();
    }

    @Override
    public String toString() {
        return "Query2Result{" +
               "timestamp=" + timestamp +
               ", locations1=" + location1 +
               ", locations2=" + location2 +
               ", locations3=" + location3 +
               ", locations4=" + location4 +
               ", locations5=" + location5 +
               ", locations6=" + location6 +
               ", locations7=" + location7 +
               ", locations8=" + location8 +
               ", locations9=" + location9 +
               ", locations10=" + location10 +
               ", temperature1=" + temperature1 +
               ", temperature2=" + temperature2 +
               ", temperature3=" + temperature3 +
               ", temperature4=" + temperature4 +
               ", temperature5=" + temperature5 +
               ", temperature6=" + temperature6 +
               ", temperature7=" + temperature7 +
               ", temperature8=" + temperature8 +
               ", temperature9=" + temperature9 +
               ", temperature10=" + temperature10 +
               '}';
    }

    @Override
    public String getKey() {
        return "query2";
    }
}
