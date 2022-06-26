package com.diagiac.flink.query1;

import com.diagiac.flink.FlinkRecord;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query1.bean.Query1Record;
import com.diagiac.flink.query1.serialize.QueryRecordDeserializer1;
import com.diagiac.flink.query1.utils.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.time.Duration;

public class Query1 extends Query {
    // kafka + flink https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/kafka/
    // watermark gen https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/event-time/generating_watermarks/

    /**
     * For those sensors having sensor_id< 10000, find the number
     * of measurements and the temperature average value
     *
     * Q1 output:
     * ts, sensor_id, count, avg_temperature
     *
     * Using a tumbling window, calculate this query:
     * – every 1 hour (event time)
     * – every 1 week (event time)
     * – from the beginning of the dataset
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        var q1 = new Query1();
        SingleOutputStreamOperator<Query1Record> d = q1.initialize();
        q1.realtimePreprocessing(d, args.length > 0 ? WindowEnum.valueOf(args[0]) : WindowEnum.Hour); // TODO: testare
        q1.sinkConfiguration();
        q1.queryConfiguration();
        q1.execute();
    }

    @Override
    public SingleOutputStreamOperator<Query1Record> initialize() {
        /* set up the Kafka source that consumes records from broker */
        var source = KafkaSource.<Query1Record>builder()
                .setBootstrapServers("kafka://kafka:9092")
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(QueryRecordDeserializer1.class))
                .build();
        var kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        return kafkaSource.filter(new RecordFilter1());
    }

    @Override
    public void queryConfiguration() {

    }

    @Override
    public void realtimePreprocessing(SingleOutputStreamOperator<? extends FlinkRecord> d, WindowEnum window) {
        var dd = (SingleOutputStreamOperator<Query1Record>) d;
        var water = dd.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Query1Record>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((queryRecord1, l) -> queryRecord1.getTimestamp().getTime()) // assign the timestamp
        );

        // Query1Record -> (sensorId, resto di Query1Record)
        var sensorKeyed = water.keyBy(Query1Record::getSensorId); // Set the sensorid as the record's key
        var windowed = sensorKeyed.window(window.getWindowStrategy());
        var aggregated = windowed.aggregate(new AverageAggregator());
        aggregated.print();
    }

    @Override
    public void sinkConfiguration() {
        /* Set up the Redis sink */
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("redis").setPort(6379).build();
    }
}
