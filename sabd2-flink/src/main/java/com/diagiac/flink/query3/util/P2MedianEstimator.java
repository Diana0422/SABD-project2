package com.diagiac.flink.query3.util;

import java.util.Arrays;

import static com.diagiac.flink.query3.util.P2MedianEstimator.InitializationStrategy.Adaptive;

/**
 * Class that exstimates the median (0.5-quantile) using the P square algorithm.
 * The only things we added is the merge() method, and we removed the possibility to get
 * a quantile other than the median
 * <p>
 * Code is based on Andrey Akinshin 2021 C#
 * <a href="https://aakinshin.net/posts/p2-quantile-estimator-adjusting-order/">implementation</a>
 */
public class P2MedianEstimator {
    private static final double prob = 0.5; // we fixed the probability to 0.5 to always get the median
    private final InitializationStrategy strategy;
    /**
     * Desired Positions
     */
    private final int[] n = new int[5];
    /**
     * Marker position
     */
    private double[] ns = new double[5];
    /**
     * Quantile Array. The central quantile is the median.
     */
    private final double[] q = new double[5];
    /**
     * Number of element computed. DO NOT USE IT TO COMPUTE Average. The merge method resets this value to max 10.
     */
    public int count;

    public enum InitializationStrategy {
        Classic,
        Adaptive
    }

    public P2MedianEstimator() {
        this(InitializationStrategy.Classic);
    }

    public P2MedianEstimator(InitializationStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * We used this method to add a new value to the estimator
     * and also to merge two estimators into one
     * @param value the value to add (e.g. the measured temperature)
     */
    public void add(double value) {
        if (count < 5) {
            q[count++] = value;
            if (count == 5) {
                Arrays.sort(q);

                for (int i = 0; i < 5; i++)
                    n[i] = i;

                if (strategy == Adaptive) {
                    ns = Arrays.copyOf(q, 5);
                    n[1] = (int) Math.round(2 * prob);
                    n[2] = (int) Math.round(4 * prob);
                    n[3] = (int) Math.round(2 + 2 * prob);
                    q[1] = ns[n[1]];
                    q[2] = ns[n[2]];
                    q[3] = ns[n[3]];
                }

                ns[0] = 0; // 0
                ns[1] = 2 * prob; // 1
                ns[2] = 4 * prob; // 2
                ns[3] = 2 + 2 * prob; // 3
                ns[4] = 4; // 4
            }

            return;
        }

        int k;
        if (value < q[0]) {
            q[0] = value;
            k = 0;
        } else if (value < q[1])
            k = 0;
        else if (value < q[2])
            k = 1;
        else if (value < q[3])
            k = 2;
        else if (value < q[4])
            k = 3;
        else {
            q[4] = value;
            k = 3;
        }

        for (int i = k + 1; i < 5; i++)
            n[i]++;
        ns[1] = count * prob / 2;
        ns[2] = count * prob;
        ns[3] = count * (1 + prob) / 2;
        ns[4] = count;


        for (int i = 1; i <= 3; i++)
            adjust(i);


        count++;
    }

    private void adjust(int i) {
        double d = ns[i] - n[i];
        if (d >= 1 && n[i + 1] - n[i] > 1 || d <= -1 && n[i - 1] - n[i] < -1) {
            int dInt = (int) Math.signum(d);
            double qs = parabolic(i, dInt);
            if (q[i - 1] < qs && qs < q[i + 1])
                q[i] = qs;
            else
                q[i] = linear(i, dInt);
            n[i] += dInt;
        }
    }

    private double parabolic(int i, double d) {
        return q[i] + d / (n[i + 1] - n[i - 1]) * (
                (n[i] - n[i - 1] + d) * (q[i + 1] - q[i]) / (n[i + 1] - n[i]) +
                (n[i + 1] - n[i] - d) * (q[i] - q[i - 1]) / (n[i] - n[i - 1])
        );
    }

    private double linear(int i, int d) {
        return q[i] + d * (q[i + d] - q[i]) / (n[i + d] - n[i]);
    }

    public double getMedian() {
        if (count == 0)
            throw new IllegalStateException("Sequence contains no elements");
        if (count <= 5) {
            Arrays.sort(q, 0, count);
            int index = (int) Math.round((count - 1) * prob);
            return q[index];
        }

        return q[2];
    }

    /**
     * Merges two P2MedianEstimator into one, keeping a good approximation of median
     * @param other the other estimator to merge
     * @return the merged estimator
     */
    public P2MedianEstimator merge(P2MedianEstimator other) {
        // with only two elements in each estimator, we get the true median.
        if (this.count == 1 && other.count == 1) {
            var p2 = new P2MedianEstimator(Adaptive);
            p2.add(this.q[0]); // we add the 2 elements in the q[] array to the new estimator
            p2.add(other.q[0]);
            return p2;
        }
        // In general, we add the all elements in the q[] array to the new estimator
        // the count of the new P2MedianEstimator will be in {0,1,...,10}
        var p2Median = new P2MedianEstimator(Adaptive);

        for (var x : this.q) {
            p2Median.add(x); // max 5 elements added
        }
        for (var x : other.q) {
            p2Median.add(x); // max 5 elements added
        }

        return p2Median;
    }

    public void clear() {
        count = 0;
    }
}
