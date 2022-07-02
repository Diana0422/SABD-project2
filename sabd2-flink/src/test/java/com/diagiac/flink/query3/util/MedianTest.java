package com.diagiac.flink.query3.util;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MedianTest {
    @Test
    @Disabled
    public void medianTest() {
        List<Double> doubles = Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5);
        Double median = MedianCalculator.medianOfMediansApproximate(doubles);
        assertEquals(trueMedian(doubles.toArray(Double[]::new)), median, 0.0001);

        doubles = Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5, 10.0, 4.2);
        median = MedianCalculator.medianOfMediansApproximate(doubles);
        doubles.sort(Comparator.naturalOrder());
        assertEquals(trueMedian(doubles.toArray(Double[]::new)), median, 0.0001);

        doubles = Arrays.asList(5.2, 6.2, 1.1, 5.4);
        median = MedianCalculator.medianOfMediansApproximate(doubles);
        assertEquals(trueMedian(doubles.toArray(Double[]::new)), median, 0.0001);

        doubles = Arrays.asList(5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3);
        median = MedianCalculator.medianOfMediansApproximate(doubles);
        doubles.sort(Comparator.naturalOrder());
        assertEquals(trueMedian(doubles.toArray(Double[]::new)), median, 0.0001);
    }

    private double trueMedian(Double[] numArray) {
        // First step: sort the array
        Arrays.sort(numArray);
        double median;
        // Second step - check if the length of the array is odd or even
        if (numArray.length % 2 == 0)
            //Third step A - if it is even get the two median values and compute the average of those
            median = (numArray[numArray.length / 2] + (double) numArray[numArray.length / 2 - 1]) / 2;
        else
            //Third step B - if it is odd, simply get the median value
            median = numArray[numArray.length / 2];
        return median;
    }
}
