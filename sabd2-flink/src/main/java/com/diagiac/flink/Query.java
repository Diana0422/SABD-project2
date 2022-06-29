package com.diagiac.flink;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class Query<T extends FlinkRecord, R extends FlinkResult> {

    protected StreamExecutionEnvironment env;
    protected String url;
    protected WindowEnum windowEnum;

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
     */
    public abstract void queryConfiguration(SingleOutputStreamOperator<T> d);

    /**
     * Settings for the sink that consumes the output of the queries
     */
    public abstract void sinkConfiguration(SingleOutputStreamOperator<R> resultsStream);

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
