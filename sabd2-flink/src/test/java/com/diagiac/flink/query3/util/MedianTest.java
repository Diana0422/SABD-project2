package com.diagiac.flink.query3.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static com.diagiac.flink.query3.util.P2MedianEstimator.InitializationStrategy.Adaptive;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MedianTest {

    static Stream<List<Double>> doubleListProvider() {
        // You can use this to get two string arguments instead of one
        // Arguments.of("argomento1", "argomento2");
        return Stream.of(
                Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5),
                Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5, 10.0, 4.2),
                Arrays.asList(5.2, 6.2, 1.1, 5.4),
                Arrays.asList(5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3),
                Arrays.asList(51.2, 16.2, 21.1, 5.44, 15.2, 66.2, 31.1, 42.3, 12.5, 22.3,5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3,5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3, 5.2, 6.2, 1.1, 42.3, 12.5, 10.0, 4.2)
        );
    }

    @MethodSource("doubleListProvider")
    @ParameterizedTest
    public void medianTest(List<Double> doubles) {
        P2MedianEstimator p = new P2MedianEstimator(Adaptive);

        for (Double d : doubles) {
            p.add(d);
        }

        double approxMedian = p.getMedian();
        double trueMedian = trueMedian(doubles);
        assertEquals(trueMedian, approxMedian, 5.0);
        System.out.println("True median: " + trueMedian + " approximate median: " + approxMedian);
    }

    private double trueMedian(List<Double> numArray) {
        // First step: sort the array
        numArray.sort(Comparator.naturalOrder());
        double median;
        // Second step - check if the length of the array is odd or even
        if (numArray.size() % 2 == 0)
            //Third step A - if it is even get the two median values and compute the average of those
            median = (numArray.get(numArray.size() / 2) + (double) numArray.get(numArray.size() / 2 - 1)) / 2;
        else
            //Third step B - if it is odd, simply get the median value
            median = numArray.get(numArray.size() / 2);
        return median;
    }
}


