package com.diagiac.flink.query2.bean;
import com.diagiac.flink.query2.bean.LocationTemperature;
import com.diagiac.flink.query2.bean.Query2Result;
import java.sql.Timestamp;
import java.util.*;

public class RankAccumulator {
    private final static int SIZE = 5;
    private final TreeSet<LocationTemperature> rankingTop;
    private final TreeSet<LocationTemperature> rankingBottom;
//    private final Map<Long, LocationTemperature> currentVisitedElems;

    public RankAccumulator() {
        // Hashmaps containing visited elements
//        this.currentVisitedElems = new HashMap<>();

        this.rankingTop = new TreeSet<>((o1, o2) -> {
            int result = o1.getAvgTemperature().compareTo(o2.getAvgTemperature());
            if (result == 0) {
                return o1.getLocation().compareTo(o2.getLocation());
            } else {
                return result;
            }
        });

        this.rankingBottom = new TreeSet<>((o1, o2) -> {
            int result = o1.getAvgTemperature().compareTo(o2.getAvgTemperature());
            if (result == 0) {
                return o1.getLocation().compareTo(o2.getLocation());
            } else {
                return result;
            }
        });

        for (int i = 1; i <= SIZE; i++) {
            this.rankingTop.add(new LocationTemperature(new Timestamp(0L), (double) -i, 0L));
        }
        for (int i = 1; i <= SIZE; i++) {
            this.rankingBottom.add(new LocationTemperature(new Timestamp(0L), (double) i * 200, 0L));
        }
    }

    public void addData(LocationTemperature data) {
        var min = this.rankingTop.first(); // get max element in rank

        if (data.getAvgTemperature() > min.getAvgTemperature()) {
            this.rankingTop.add(data); // add new element that will be new max
            this.rankingTop.remove(min);
//            this.currentVisitedElems.put(data.getLocation(), data);
        }

        var max = this.rankingBottom.last();

        if (data.getAvgTemperature() < max.getAvgTemperature()) {
            this.rankingBottom.add(data);
            this.rankingBottom.remove(max);
//            this.currentVisitedElems.put(data.getLocation(), data);
        }
    }

    public Query2Result getResult() {
        NavigableSet<LocationTemperature> rankResultTop = this.rankingTop;
        return new Query2Result(rankResultTop.first().getTimestamp(), new ArrayList<LocationTemperature>(rankResultTop), new ArrayList<>(this.rankingBottom));
    }
}

