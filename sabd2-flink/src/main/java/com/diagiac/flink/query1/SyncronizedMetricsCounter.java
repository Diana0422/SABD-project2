package com.diagiac.flink.query1;


import static org.apache.commons.math3.util.Precision.round;

public class SyncronizedMetricsCounter {
    // tuples counter
    private static long counter = 0L;
    // first output time
    private static long startTime;

    /**
     * Called when a new observation occurred.
     * Used to compute mean throughput and latency
     *
     * @param context type of query and window
     */
    public static synchronized void incrementCounter(String context) {
        if (counter == 0L) {
            // set starting time
            startTime = System.currentTimeMillis();
            System.out.println("Initialized time");
        }

        // increment counter every time a new observation occurred
        counter++;

        double totalLatency = System.currentTimeMillis() - startTime;

        // mean throughput
        double throughput = counter / totalLatency;
        // mean latency
        double latency = totalLatency / counter;

        System.out.println(context + "::" + "Mean throughput: " + round(throughput, 5) + " tuple/ms\n" + "Mean latency: "+
                round(latency, 3) + "ms\n" + "Counter: " + counter + " tuples\n" + "Total latency: " + totalLatency + " ms");
    }
}
