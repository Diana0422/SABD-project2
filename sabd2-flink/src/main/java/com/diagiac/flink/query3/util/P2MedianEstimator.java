package com.diagiac.flink.query3.util;

import java.util.Arrays;

/**
 * Class that exstimates the median (0.5-quantile) using the P square algorithm.
 *
 * Code is based on Andrey Akinshin 2021 C#
 * <a href="https://aakinshin.net/posts/p2-quantile-estimator-adjusting-order/">implementation</a>
 */
public class P2MedianEstimator {
    private static final double prob = 0.5;
    private final InitializationStrategy strategy;
    private final int[] n = new int[5];
    private double[] ns = new double[5];
    private final double[] q = new double[5];

    public int count;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

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

    public void add(double value) {
        if (count < 5) {
            q[count++] = value;
            if (count == 5) {
                Arrays.sort(q);

                for (int i = 0; i < 5; i++)
                    n[i] = i;

                if (strategy == InitializationStrategy.Adaptive) {
                    ns = Arrays.copyOf(q, 5);
                    n[1] = (int) Math.round(2 * prob);
                    n[2] = (int) Math.round(4 * prob);
                    n[3] = (int) Math.round(2 + 2 * prob);
                    q[1] = ns[n[1]];
                    q[2] = ns[n[2]];
                    q[3] = ns[n[3]];
                }

                ns[0] = 0;
                ns[1] = 2 * prob;
                ns[2] = 4 * prob;
                ns[3] = 2 + 2 * prob;
                ns[4] = 4;
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
            Adjust(i);


        count++;
    }

    private void Adjust(int i) {
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

    public void clear() {
        count = 0;
    }
}
