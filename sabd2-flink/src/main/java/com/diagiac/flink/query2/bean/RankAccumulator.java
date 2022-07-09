package com.diagiac.flink.query2.bean;
import java.sql.Timestamp;
import java.util.*;

public class RankAccumulator {
    private final static int SIZE = 5;
    private final TreeSet<TemperatureMeasure> rankingTop;
    private final TreeSet<TemperatureMeasure> rankingBottom;


    public RankAccumulator() {
        this.rankingTop = new TreeSet<>((o1, o2) -> {
            int result = o1.getAvgTemperature().compareTo(o2.getAvgTemperature());
            if (result == 0) {
                return o1.getSensorId().compareTo(o2.getSensorId());
            } else {
                return result;
            }
        });

        this.rankingBottom = new TreeSet<>((o1, o2) -> {
            int result = o1.getAvgTemperature().compareTo(o2.getAvgTemperature());
            if (result == 0) {
                return o1.getSensorId().compareTo(o2.getSensorId());
            } else {
                return result;
            }
        });

        for (int i = 1; i <= SIZE; i++) {
            this.rankingTop.add(new TemperatureMeasure(new Timestamp(0L), (double) -i, 0L));
        }
        for (int i = 1; i <= SIZE; i++) {
            this.rankingBottom.add(new TemperatureMeasure(new Timestamp(0L), (double) i * 200, 0L));
        }
    }

    public void addData(TemperatureMeasure data) {
        var min = this.rankingTop.first(); // get max element in rank

        if (data.getAvgTemperature() > min.getAvgTemperature()) {
            this.rankingTop.add(data); // add new element that will be new max
            this.rankingTop.remove(min);
        }

        var max = this.rankingBottom.last();

        if (data.getAvgTemperature() < max.getAvgTemperature()) {
            this.rankingBottom.add(data);
            this.rankingBottom.remove(max);
        }
    }

    public Query2Result getResult() {
        NavigableSet<TemperatureMeasure> rankResultTop = this.rankingTop;
        return new Query2Result(rankResultTop.first().getTimestamp(), new ArrayList<>(rankResultTop), new ArrayList<>(this.rankingBottom));
    }
}

