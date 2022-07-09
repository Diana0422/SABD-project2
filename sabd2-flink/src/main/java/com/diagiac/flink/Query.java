package com.diagiac.flink;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Abstract class that represents a generic Flink query.
 * It has method to separate every part of a query.
 * The execute method is implemented and must be called to execute the query on the Flink Cluster.
 * @param <T> with input param T (e.g. Query1Record)
 * @param <R> and output param R (e.b. Query1Result)
 */
public abstract class Query<T extends FlinkRecord, R extends FlinkResult> {

    protected StreamExecutionEnvironment env;
    protected String url;

    public Query(){
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * Settings for the query (e.g. window, parallelism, watermark, checkpointing)
     *
     * @return
     */
    public abstract SingleOutputStreamOperator<T> sourceConfigurationAndFiltering();

    /**
     * Implements the query to execute
     *
     * @return
     */
    public abstract SingleOutputStreamOperator<R> queryConfiguration(SingleOutputStreamOperator<T> d, WindowEnum windowAssigner, String opName);

    /**
     * Settings for the sink that consumes the output of the queries
     */
    public abstract void sinkConfiguration(SingleOutputStreamOperator<R> resultsStream, WindowEnum windowType);

    /**
     * Runs the Flink job
     */
    public void execute() {
        try {
            this.env.execute(this.getClass().getSimpleName());
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
