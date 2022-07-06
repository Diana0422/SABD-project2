package com.diagiac.flink.query2;

import com.diagiac.flink.MetricRichMapFunction;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query1.bean.Query1Result;
import com.diagiac.flink.query1.serialize.QueryResultSerializer1;
import com.diagiac.flink.query2.bean.LocationTemperature;
import com.diagiac.flink.query2.bean.Query2Record;
import com.diagiac.flink.query2.bean.Query2Result;
import com.diagiac.flink.query2.serialize.QueryRecordDeserializer2;
import com.diagiac.flink.query2.serialize.QueryResultSerializer2;
import com.diagiac.flink.query2.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

public class Query2 extends Query<Query2Record, Query2Result> {

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
//        var window = args.length > 0 ? WindowEnum.valueOf(args[0]) : WindowEnum.Hour;
        var url = args.length > 1 ? args[1] : "127.0.0.1:29092";
        var q2 = new Query2(url);
        var d = q2.sourceConfigurationAndFiltering();
        var resultStream = q2.queryConfiguration(d, WindowEnum.Hour, "query2-hour"); // TODO: testare
        var resultStream2 = q2.queryConfiguration(d, WindowEnum.Day, "query2-day"); // TODO: testare
        var resultStream3 = q2.queryConfiguration(d, WindowEnum.Week, "query2-week"); // TODO: testare
        q2.sinkConfiguration(resultStream, WindowEnum.Hour);
        q2.sinkConfiguration(resultStream2, WindowEnum.Day);
        q2.sinkConfiguration(resultStream3, WindowEnum.Week);
        q2.execute();
    }

    @Override
    public SingleOutputStreamOperator<Query2Record> sourceConfigurationAndFiltering() {
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
    public SingleOutputStreamOperator<Query2Result> queryConfiguration(SingleOutputStreamOperator<Query2Record> stream, WindowEnum windowAssigner, String opName) {
        return stream.keyBy(Query2Record::getLocation) // group by location
                .window(windowAssigner.getWindowStrategy())// setting window strategy (hour, day, week)
                .aggregate(new AverageAggregate2(), new Query2ProcessWindowFunction()) // compute mean incrementally for elements that arrive
                .keyBy(LocationTemperature::getTimestamp) // group by timestamp, which is the same for all the element of the window
                .window(windowAssigner.getWindowStrategy()) // conceptually like windowAll, so it is parallelizable !!!
                .aggregate(new RankAggregate(), new RankingProcessWindowFunction()) // compute the top5 and bottom5 for the single window, for each partition
                .map(new MetricRichMapFunction<>()) // metrics (throughput and latency)
                .name(opName);
    }

    @Override
    public void sinkConfiguration(SingleOutputStreamOperator<Query2Result> resultStream, WindowEnum windowType) {
        /* Set up the redis sink */
        resultStream.addSink(new RedisHashSink2(windowType));
        /* Set up stdOut Sink */
        resultStream.print();
        /* Set up Kafka sink */
        var sink = KafkaSink.<Query2Result>builder()
                .setBootstrapServers(url)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("query2-"+windowType.name())
                        .setKafkaValueSerializer(QueryResultSerializer2.class)
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        resultStream.sinkTo(sink);
    }
}
