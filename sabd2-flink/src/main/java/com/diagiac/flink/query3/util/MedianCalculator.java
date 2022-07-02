package com.diagiac.flink.query3.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MedianCalculator {
    public static Double medianOfMedians(List<Double> doubles) {
        List<List<Double>> subLists = new ArrayList<>();
        // 1. SubLists of 5 elements
        if (doubles.size() < 5) {
            subLists.add(doubles);
        } else {
            for (int i = 0; i < doubles.size() / 5; i++) {
                if (doubles.size() > i + 5) {
                    subLists.add(doubles.subList(i, i + 5));
                } else {
                    subLists.add(doubles.subList(i, doubles.size()));
                }
            }
        }

        var medians = new ArrayList<Double>();
        // 2. Sorting the sublists and getting medians
        for (List<Double> subList : subLists) {
            subList.sort(Comparator.naturalOrder());
            if (subList.size() % 2 == 0) {
                double floor = Math.floor((double) subList.size() / 2);
                medians.add(subList.get((int) floor));
            } else {
                medians.add(subList.get(subList.size() / 2));
            }
        }
        Double approxMedian;
        if (subLists.size() > 1)
            approxMedian = medianOfMedians(medians);
        else
            approxMedian = subLists.get(0).get(subLists.get(0).size() / 2);
        return approxMedian;
    }
}
