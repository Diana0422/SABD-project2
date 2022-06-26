package com.diagiac.flink.query2;

import com.diagiac.flink.FlinkRecord;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query2.bean.Query2Record;
import com.diagiac.flink.query2.util.AverageAggregator2;
import com.diagiac.flink.query2.util.SortKeyedProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

public class Query2 extends Query {

    /**
     * Find the real-time top-5 ranking of locations (location) having
     * the highest *average* temperature and the top-5 ranking of locations (location)
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
        SingleOutputStreamOperator<Query2Record> d = q2.initialize();
        q2.realtimePreprocessing(d, WindowEnum.valueOf(args[0])); // TODO: testare
        q2.sinkConfiguration();
        q2.queryConfiguration();
        q2.execute();
    }

    @Override
    public SingleOutputStreamOperator<Query2Record> initialize() {
        var kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka://kafka:9092")
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        /* // Checkpointing - Start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/frauddetection/checkpoint");
        */

        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        var filtered = dataStreamSource.filter(new RecordFilter2());
        return filtered.map(new RecordMapper2());
    }

    @Override
    public void queryConfiguration() {

    }

    @Override
    public void realtimePreprocessing(SingleOutputStreamOperator<? extends FlinkRecord> d, WindowEnum window) {
        var dd = (SingleOutputStreamOperator<Query2Record>) d;
        var water = dd.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Query2Record>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((queryRecord2, l) -> queryRecord2.getTimestamp().getTime()) // assign the timestamp
        );

        // Query2Record -> (Location, resto di query2Record)
        var locationKey = water.keyBy(Query2Record::getLocation);

        var windowed = locationKey.window(window.getWindowStrategy());

        // (Location, resto di query2Record) -> (Location, avgTemperature) nella finestra
        var aggregated = windowed.aggregate(new AverageAggregator2());

        var windowedAll = aggregated.windowAll(window.getWindowStrategy());
        var processed = windowedAll.process(new SortKeyedProcessFunction());

        processed.print();

    }

    @Override
    public void sinkConfiguration() {
        /* Set up the Redis sink */
        // FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("redis").setPort(6379).build();
    }
}
