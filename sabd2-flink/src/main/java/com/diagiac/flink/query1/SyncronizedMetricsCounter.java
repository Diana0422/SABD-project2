package com.diagiac.flink.query1;


import java.io.Serializable;

import static org.apache.commons.math3.util.Precision.round;


@Deprecated
public class SyncronizedMetricsCounter implements Serializable {
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
    public synchronized void incrementCounter(String context) {
        if (counter == 0L) {
            // set starting time
            startTime = System.currentTimeMillis();
            System.out.println("Initialized time");
        }

        // increment counter every time a new observation occurred
        counter++;

        double totalLatency = System.currentTimeMillis() - startTime;

        // mean throughput TUPLE TOTALI FINO AD ORA / TEMPO TOTALE FINO AD ORA
        // TEMPO TRA DUE TUPLE IN USCITA CONSECUTIVE
        double throughput = counter / totalLatency; // TUPLE AL MS
        // mean latency
        double latency = totalLatency / counter;

        System.out.println(context + "::" + "Mean throughput: " + round(throughput, 5) + " tuple/ms\n" + "Mean latency: "+
                round(latency, 3) + "ms\n" + "Counter: " + counter + " tuples\n" + "Total latency: " + totalLatency + " ms");
    }
}
