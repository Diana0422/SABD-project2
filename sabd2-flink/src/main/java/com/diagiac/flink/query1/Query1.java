package com.diagiac.flink.query1;

import com.diagiac.flink.MetricRichMapFunction;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query1.bean.Query1Record;
import com.diagiac.flink.query1.bean.Query1Result;
import com.diagiac.flink.query1.serialize.QueryRecordDeserializer1;
import com.diagiac.flink.query1.utils.AverageAggregator;
import com.diagiac.flink.query1.utils.RecordFilter1;
import com.diagiac.flink.redis.TheRedisMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.time.Duration;

public class Query1 extends Query<Query1Record, Query1Result> {
    public Query1(String url, WindowEnum windowAssigner) {
        this.url = url;
        this.windowEnum = windowAssigner;
    }
    // kafka + flink https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/kafka/
    // watermark gen https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/event-time/generating_watermarks/

    /**
     * For those sensors having sensor_id< 10000, find the number
     * of measurements and the temperature average value
     * <p>
     * Q1 output:
     * ts, sensor_id, count, avg_temperature
     * <p>
     * Using a tumbling window, calculate this query:
     * – every 1 hour (event time)
     * – every 1 week (event time)
     * – from the beginning of the dataset
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        var url = args.length > 1 ? args[1] : "127.0.0.1:29092";
        var windowAssigner = args.length > 0 ? WindowEnum.valueOf(args[0]) : WindowEnum.Hour;
        var q1 = new Query1(url, windowAssigner);
        SingleOutputStreamOperator<Query1Record> d = q1.sourceConfigurationAndFiltering();
        System.out.println("Filtered Data");
        var resultStream = q1.queryConfiguration(d); // TODO: testare
        q1.sinkConfiguration(resultStream);
        q1.execute();
    }

    @Override
    public SingleOutputStreamOperator<Query1Record> sourceConfigurationAndFiltering() {
        /* set up the Kafka source that consumes records from broker */
        var source = KafkaSource.<Query1Record>builder()
                .setBootstrapServers(this.url)
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(QueryRecordDeserializer1.class))
                .build();
        var kafkaSource = env.fromSource(source, WatermarkStrategy.<Query1Record>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((queryRecord1, l) -> queryRecord1.getTimestamp().getTime()), "Kafka Source")
                .setParallelism(1);
        System.out.println("Consumed data from Kafka");
        return kafkaSource.filter(new RecordFilter1());
    }

    @Override
    public SingleOutputStreamOperator<Query1Result> queryConfiguration(SingleOutputStreamOperator<Query1Record> d) {
        // Query1Record -> (sensorId, resto di Query1Record)
        System.out.println("Query1 start processing");
        var sensorKeyed = d.keyBy(Query1Record::getSensorId); // Set the sensorid as the record's key
        System.out.println("after: keyBy sensor id");
        var windowed = sensorKeyed.window(windowEnum.getWindowStrategy());
        System.out.println("Selected Window: "+windowEnum.getWindowStrategy());
        var aggregated = windowed.aggregate(new AverageAggregator());
        System.out.println("After aggregated");
//        aggregated.map(new MetricRichMapFunction<>()); // just for metrics
//        System.out.println("After metrics map");
        return aggregated;
    }

    @Override
    public void sinkConfiguration(SingleOutputStreamOperator<Query1Result> resultStream) {
        /* Set up the redis sink */
        System.out.println("Setting sinks:");
//        var conf = new FlinkJedisPoolConfig.Builder().setHost("redis-cache").setPort(6379).build();
//        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query1", "timestamp", "getTimestamp")));
//        System.out.println("Setting sinks: sink1 redis");
//        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query1", "sensorId", "getSensorId")));
//        System.out.println("Setting sinks: sink2 redis");
//        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query1", "count", "getCount")));
//        System.out.println("Setting sinks: sink3 redis");
//        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query1", "averageTemperature", "getAvgTemperature")));
//        System.out.println("Setting sinks: sink4 redis");
        /* Set up stdOut Sink */
        System.out.println("Setting sinks: sink stdout");
        resultStream.print();
    }
}
