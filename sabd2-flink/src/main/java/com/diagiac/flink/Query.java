package com.diagiac.flink;

import com.diagiac.flink.query2.bean.Query2Record;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class Query {

    protected StreamExecutionEnvironment env;

    public Query(){
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * Settings for the query (e.g. window, parallelism, watermark, checkpointing)
     *
     * @return
     */
    public abstract SingleOutputStreamOperator<? extends FlinkRecord> initialize();

    /**
     * Implements the query to execute
     */
    public abstract void queryConfiguration();

    /**
     * Settings for the source from which data comes from.
     */
    public abstract void realtimePreprocessing(SingleOutputStreamOperator<? extends FlinkRecord> d, WindowEnum window);

    /**
     * Settings for the sink that consumes the output of the queries
     */
    public abstract void sinkConfiguration();

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
