package com.diagiac.flink.query2;

import com.diagiac.flink.Query;
import com.diagiac.flink.SensorRecord;
import com.diagiac.flink.query2.bean.Query2Record;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class Query2 extends Query {

    /**
     * Find the real-time top-5 ranking of locations (location) having
     * the highest average temperature and the top-5 ranking of locations (location)
     * having the lowest average temperature
     * <p>
     * Q2 output:
     * ts, location1, avg_temp1, ... location5, avg_temp5, location6, avg_temp6, ... location10, avg_temp10
     * <p>
     * Using a tumbling window, calculate this query:
     * – every 1 hour (event time)
     * – every 1 day (event time)
     * – every 1 week (event time)
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        var q2 = new Query2();
        DataStreamSource<SensorRecord> d = q2.initialize();
        q2.realtimePreprocessing(d);
        q2.sinkConfiguration();
        q2.queryConfiguration();
        q2.execute();
    }

    @Override
    public DataStreamSource<SensorRecord> initialize() {
        KafkaSource<SensorRecord> kafkaSource = KafkaSource.<SensorRecord>builder()
                .setBootstrapServers("kafka://kafka:9092")
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                // .setValueOnlyDeserializer()
                .build();

        /* // Checkpointing - Start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/frauddetection/checkpoint");
        */

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    @Override
    public void queryConfiguration() {

    }

    @Override
    public void realtimePreprocessing(DataStreamSource<SensorRecord> d) {
        SingleOutputStreamOperator<Query2Record> map = d.map(Query2Record::new);
        map.print();
    }

    @Override
    public void sinkConfiguration() {
        /* Set up the Redis sink */
        // FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("redis").setPort(6379).build();
    }
}
