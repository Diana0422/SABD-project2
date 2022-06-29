package com.diagiac.flink.query2;

import com.diagiac.flink.FlinkRecord;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query2.bean.Query2Record;
import com.diagiac.flink.query2.serialize.QueryRecordDeserializer2;
import com.diagiac.flink.query2.util.AverageAggregator2;
import com.diagiac.flink.query2.util.RecordFilter2;
import com.diagiac.flink.query2.util.SortKeyedProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

public class Query2 extends Query {

    public Query2(String url) {
        this.url = url;
    }

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
        var url = args.length > 1 ? args[1] : "127.0.0.1:29092";
        var q2 = new Query2(url);
        SingleOutputStreamOperator<Query2Record> d =  q2.initialize();
        q2.queryConfiguration(d, args.length > 0 ? WindowEnum.valueOf(args[0]) : WindowEnum.Hour); // TODO: testare
        q2.sinkConfiguration();
        q2.execute();
    }

    @Override
    public SingleOutputStreamOperator<Query2Record> initialize() {
        var source = KafkaSource.<Query2Record>builder()
                .setBootstrapServers(this.url) // kafka://kafka:9092,
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(QueryRecordDeserializer2.class))
                .build();

        /* // Checkpointing - Start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/frauddetection/checkpoint");
        */

        var kafkaSource = env.fromSource(source, WatermarkStrategy.<Query2Record>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner((queryRecord2, l) -> queryRecord2.getTimestamp().getTime()), "Kafka Source")
                .setParallelism(1);
        return kafkaSource.filter(new RecordFilter2());
    }

    @Override
    public void queryConfiguration(SingleOutputStreamOperator<? extends FlinkRecord> d, WindowEnum window) {
        var dd = (SingleOutputStreamOperator<Query2Record>) d;

        // Query2Record -> (Location, resto di query2Record)
        var locationKeyed = dd.keyBy(Query2Record::getLocation);
        var windowed = locationKeyed.window(window.getWindowStrategy());

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
