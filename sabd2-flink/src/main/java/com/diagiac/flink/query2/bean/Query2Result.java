package com.diagiac.flink.query2.bean;

import com.diagiac.flink.FlinkResult;
import com.diagiac.flink.WindowEnum;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;
import java.util.List;

@Data
@AllArgsConstructor
public class Query2Result implements FlinkResult {
    private Timestamp timestamp;
    private Long location1; // this is the sensor relative to the max temperature location
    private Long location2;
    private Long location3;
    private Long location4;
    private Long location5;
    private Long location6; // this is the sensor id relative to the min temperature location
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
        location1 = maxTemperatures.get(4).getSensorId();
        location2 = maxTemperatures.get(3).getSensorId();
        location3 = maxTemperatures.get(2).getSensorId();
        location4 = maxTemperatures.get(1).getSensorId();
        location5 = maxTemperatures.get(0).getSensorId();

        location6 = minTemperatures.get(0).getSensorId();
        location7 = minTemperatures.get(1).getSensorId();
        location8 = minTemperatures.get(2).getSensorId();
        location9 = minTemperatures.get(3).getSensorId();
        location10 = minTemperatures.get(4).getSensorId();

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
    public String getRedisKey(WindowEnum windowType) {
        return windowType.name() + ":" + "query2";
    }

    public String toStringCSV() {
        return timestamp.toString()+","
                +location1.toString()+","
                +temperature1.toString()+","
                +location2.toString()+","
                +temperature2.toString()+","
                +location3.toString()+","
                +temperature3.toString()+","
                +location4.toString()+","
                +temperature4.toString()+","
                +location5.toString()+","
                +temperature5.toString()+","
                +location6.toString()+","
                +temperature6.toString()+","
                +location7.toString()+","
                +temperature7.toString()+","
                +location8.toString()+","
                +temperature8.toString()+","
                +location9.toString()+","
                +temperature9.toString()+","
                +location10.toString()+","
                +temperature10+"\n";
    }
}
