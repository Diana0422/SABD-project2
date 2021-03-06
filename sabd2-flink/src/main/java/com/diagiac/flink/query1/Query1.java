package com.diagiac.flink.query1;

import com.diagiac.flink.MetricRichMapFunction;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query1.bean.Query1Record;
import com.diagiac.flink.query1.bean.Query1Result;
import com.diagiac.flink.query1.serialize.QueryRecordDeserializer1;
import com.diagiac.flink.query1.serialize.QueryResultSerializer1;
import com.diagiac.flink.query1.utils.AverageAggregate1;
import com.diagiac.flink.query1.utils.RecordFilter1;
import com.diagiac.flink.query1.utils.Query1ProcessWindowFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

import static com.diagiac.flink.Constants.KAFKA_SINK_ENABLED;

public class Query1 extends Query<Query1Record, Query1Result> {
    public Query1(String url) {
        this.url = url;
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
        var url = args.length > 0 ? args[0] : "127.0.0.1:29092";
        var q1 = new Query1(url);
        SingleOutputStreamOperator<Query1Record> d = q1.sourceConfigurationAndFiltering();
        var resultStream = q1.queryConfiguration(d, WindowEnum.Hour, "query1-hour");
        var resultStream2 = q1.queryConfiguration(d, WindowEnum.Week, "query1-week");
        var resultStream3 = q1.queryConfiguration(d, WindowEnum.FromStart, "query1-start");
        q1.sinkConfiguration(resultStream, WindowEnum.Hour);
        q1.sinkConfiguration(resultStream2, WindowEnum.Week);
        q1.sinkConfiguration(resultStream3, WindowEnum.FromStart);
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
        return kafkaSource.filter(new RecordFilter1());
    }

    @Override
    public SingleOutputStreamOperator<Query1Result> queryConfiguration(SingleOutputStreamOperator<Query1Record> stream, WindowEnum windowAssigner, String opName) {
        // Query1Record -> (sensorId, resto di Query1Record)
        return stream
                .keyBy(Query1Record::getSensorId) // Set the sensorid as the record's key
                .window(windowAssigner.getWindowStrategy()) // Set the window strategy
                .aggregate(new AverageAggregate1(), new Query1ProcessWindowFunction())
                .map(new MetricRichMapFunction<>())// Aggregate function to calculate average, ProcessWindowFunction to unify timestamp
                .name(opName);
    }

    @Override
    public void sinkConfiguration(SingleOutputStreamOperator<Query1Result> resultStream, WindowEnum windowType) {
        /* Set up the redis sink */
        resultStream.addSink(new RedisHashSink1(windowType));
        /* Set up stdOut Sink */
        resultStream.print();

        if (KAFKA_SINK_ENABLED) {
            /* Set up Kafka sink */
            var sink = KafkaSink.<Query1Result>builder()
                    .setBootstrapServers(url)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("query1-" + windowType.name())
                            .setKafkaValueSerializer(QueryResultSerializer1.class)
                            .build()
                    )
                    .build();

            resultStream.sinkTo(sink);
        }
    }
}
