package com.diagiac.flink.query3.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MedianTest {
    @Test
    public void medianTest() {
        List<Double> doubles = Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5);
        Double median = MedianCalculator.medianOfMedians(doubles);
        assertEquals(6.2, median);

        doubles = Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5, 10.0, 4.2);
        median = MedianCalculator.medianOfMedians(doubles);
        doubles.sort(Comparator.naturalOrder());
        assertEquals(doubles.get(3), median);

        doubles = Arrays.asList(5.2, 6.2, 1.1, 5.4);
        median = MedianCalculator.medianOfMedians(doubles);
        assertEquals(5.4, median);

        doubles = Arrays.asList(5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3);
        median = MedianCalculator.medianOfMedians(doubles);
        doubles.sort(Comparator.naturalOrder());
        assertEquals(doubles.get(4), median);
    }
}
