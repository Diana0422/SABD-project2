package com.diagiac.flink.query2.bean;

import lombok.Data;

import java.sql.Timestamp;
import java.util.List;

@Data
public class Query2Result {
    private Timestamp timestamp;
    private Long locations1;
    private Long locations2;
    private Long locations3;
    private Long locations4;
    private Long locations5;
    private Long locations6;
    private Long locations7;
    private Long locations8;
    private Long locations9;
    private Long locations10;

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
        locations1 = maxTemperatures.get(4).getLocation();
        locations2 = maxTemperatures.get(3).getLocation();
        locations3 = maxTemperatures.get(2).getLocation();
        locations4 = maxTemperatures.get(1).getLocation();
        locations5 = maxTemperatures.get(0).getLocation();

        locations6 = minTemperatures.get(0).getLocation();
        locations7 = minTemperatures.get(1).getLocation();
        locations8 = minTemperatures.get(2).getLocation();
        locations9 = minTemperatures.get(3).getLocation();
        locations10 = minTemperatures.get(4).getLocation();

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
                ", locations1=" + locations1 +
                ", locations2=" + locations2 +
                ", locations3=" + locations3 +
                ", locations4=" + locations4 +
                ", locations5=" + locations5 +
                ", locations6=" + locations6 +
                ", locations7=" + locations7 +
                ", locations8=" + locations8 +
                ", locations9=" + locations9 +
                ", locations10=" + locations10 +
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
}
