package com.diagiac.flink.query3.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static com.diagiac.flink.query3.util.P2MedianEstimator.InitializationStrategy.Adaptive;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Some parameterized test on the approximate median
 */
public class MedianTest {
    private static final double DELTA = 100.0;

    /**
     * Parameters for medianTest
     * @return
     */
    static Stream<List<Double>> doubleListProvider() {
        // You can use this to get two string arguments instead of one
        // Arguments.of("argomento1", "argomento2");
        return Stream.of(
                Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5),
                Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5, 10.0, 4.2),
                Arrays.asList(5.2, 6.2, 1.1, 5.4),
                Arrays.asList(5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3),
                Arrays.asList(51.2, 16.2, 21.1, 5.44, 15.2, 66.2, 31.1, 42.3, 12.5, 22.3, 5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3, 5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3, 5.2, 6.2, 1.1, 42.3, 12.5, 10.0, 4.2)
        );
    }

    /**
     * Parameters for medianSplitTest and doubleMergeMedianTest
     * @return
     */
    static Stream<Arguments> doubleSplitListProvider() {
        // You can use this to get two string arguments instead of one
        // Arguments.of("argomento1", "argomento2");
        return Stream.of(
                Arguments.of(Arrays.asList(51.2, 16.2, 21.1, 5.44, 15.2, 66.2, 31.1, 42.3, 12.5, 22.3, 5.2, 6.2, 1.1, 5.4, 5.2, 6.2), Arrays.asList(1.1, 42.3, 12.5, 22.3, 5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3, 5.2, 6.2, 1.1, 42.3, 12.5, 10.0, 4.2)),
                Arguments.of(Arrays.asList(5.2, 6.2, 1.1, 5.4, 5.2, 6.2, 1.1, 42.3, 12.5, 22.3), Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5, 10.0, 4.2)),
                Arguments.of(Arrays.asList(5.2), Arrays.asList(6.2)),
                Arguments.of(Arrays.asList(5.2,6.7), Arrays.asList(76.2)),
                Arguments.of(Arrays.asList(15.2, 66.2, 31.1,55.5, 42.3, 12.5, 22.3, 5.2, 6.2, 1.1, 5.4), Arrays.asList(6.2, 2.3, 44.5,88.56,33.3,2.67,215.3, 8.15, 1000.0, 9.2, 666.0))
        );
    }

    /**
     * Checks the estimator implementation and compares it to the true median
     * @param doubles
     */
    @MethodSource("doubleListProvider")
    @ParameterizedTest
    public void medianTest(List<Double> doubles) {
        P2MedianEstimator p = new P2MedianEstimator(Adaptive);

        for (Double d : doubles) {
            p.add(d);
        }

        double approxMedian = p.getMedian();
        double trueMedian = trueMedian(doubles);
        assertEquals(trueMedian, approxMedian, DELTA);
        System.out.println("True median: " + trueMedian + " approximate median: " + approxMedian);
    }

    /**
     * Test for the merge method that we added.
     * @param doubles1 see doubleSplitListProvider
     * @param doubles2 see doubleSplitListProvider
     */
    @MethodSource("doubleSplitListProvider")
    @ParameterizedTest
    public void medianSplitTest(List<Double> doubles1, List<Double> doubles2) {
        // Initialize the two estimator to merge and the single estimator to compare it with the merged
        P2MedianEstimator p1 = new P2MedianEstimator(Adaptive);
        P2MedianEstimator p2 = new P2MedianEstimator(Adaptive);
        P2MedianEstimator pTot = new P2MedianEstimator(Adaptive);

        for (Double d : doubles1) {
            p1.add(d);
            pTot.add(d);
        }

        for (Double d : doubles2) {
            p2.add(d);
            pTot.add(d);
        }

        // only to check
        double approxMedian1 = p1.getMedian();
        double approxMedian2 = p2.getMedian();
        System.out.println("approxMedian1 = " + approxMedian1);
        System.out.println("approxMedian2 = " + approxMedian2);

        // we check the merged and the inverse merged median
        double totMedian = pTot.getMedian();
        double mergeMedian = p1.merge(p2).getMedian();
        double inverseMedian = p2.merge(p1).getMedian();
        // we also compare all with the true median of the entire list
        List<Double> completeArray = new ArrayList<>();
        completeArray.addAll(doubles1);
        completeArray.addAll(doubles2);

        System.out.println("True approximate median: " + totMedian + " merged median: " + mergeMedian +
                           "\ninverse approximate median: " + inverseMedian + " true Median: " + trueMedian(completeArray));

        assertEquals(totMedian, inverseMedian, DELTA, "the total median is MUCH different from the merge median");
        assertEquals(totMedian, mergeMedian, DELTA, "the total median is MUCH different from the merge median");
    }

    /**
     * Test for the merge method that we added. In this case it compares
     * two chained merge()s with the single estimator and the trueMedian!
     * @param doubles1 see doubleSplitListProvider
     * @param doubles2 see doubleSplitListProvider
     */
    @MethodSource("doubleSplitListProvider")
    @ParameterizedTest
    public void doubleMergeMedianTest(List<Double> doubles1, List<Double> doubles2){
        P2MedianEstimator p1 = new P2MedianEstimator(Adaptive);
        P2MedianEstimator p2 = new P2MedianEstimator(Adaptive);
        P2MedianEstimator p3 = new P2MedianEstimator(Adaptive);
        P2MedianEstimator pTot = new P2MedianEstimator(Adaptive);

        for (Double d : doubles1) {
            p1.add(d);
            pTot.add(d);
        }

        for (Double d : doubles2) {
            p2.add(d);
            pTot.add(d);
        }

        List<Double> fixedDoubles = Arrays.asList(5.2, 6.2, 1.1, 42.3, 12.5);
        for (Double fixedDouble : fixedDoubles) {
            p3.add(fixedDouble);
            pTot.add(fixedDouble);
        }

        List<Double> completeArray = new ArrayList<>();
        completeArray.addAll(doubles1);
        completeArray.addAll(doubles2);
        completeArray.addAll(fixedDoubles);
        // this is the double merge
        double doubleMerge = p1.merge(p2).merge(p3).getMedian();
        double approxMedian = pTot.getMedian();
        System.out.println(" double merged median: " + doubleMerge + " true median: " + trueMedian(completeArray) + " approximate median: " + approxMedian);
        assertEquals(approxMedian, doubleMerge, DELTA, "the total median is different from the merge median");
    }


    /**
     * Utility method to compute the true median and compare with the approximate one
     * @param doubleList a list of numbers
     * @return the true median of the numbers
     */
    private double trueMedian(List<Double> doubleList) {
        // First step: sort the array
        doubleList.sort(Comparator.naturalOrder());
        double median;
        // Second step - check if the length of the array is odd or even
        if (doubleList.size() % 2 == 0)
            //Third step A - if it is even get the two median values and compute the average of those
            median = (doubleList.get(doubleList.size() / 2) + (double) doubleList.get(doubleList.size() / 2 - 1)) / 2;
        else
            //Third step B - if it is odd, simply get the median value
            median = doubleList.get(doubleList.size() / 2);
        return median;
    }
}


