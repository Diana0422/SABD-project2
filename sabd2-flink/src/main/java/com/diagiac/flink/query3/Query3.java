package com.diagiac.flink.query3;

import com.diagiac.flink.MetricRichMapFunction;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query3.bean.CellAvgMedianTemperature;
import com.diagiac.flink.query3.bean.Query3Record;
import com.diagiac.flink.query3.bean.Query3Result;
import com.diagiac.flink.query3.serialize.QueryRecordDeserializer3;
import com.diagiac.flink.query3.serialize.QueryResultSerializer3;
import com.diagiac.flink.query3.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

import static com.diagiac.flink.Constants.KAFKA_SINK_ENABLED;

public class Query3 extends Query<Query3Record, Query3Result> {

    public Query3(String url) {
        this.url = url;
    }


    /**
     * Consider the latitude and longitude coordinates within the geographic area
     * which is identified from the latitude and longitude coordinates (38°, 2°) and (58°, 30°).
     * <p>
     * Divide this area using a 4x4 grid and identify each grid cell from the top-left to bottom-right corners
     * using the name "cell_X", where X is the cell id from 0 to 15. For each cell, find
     * <p>
     * - the average temperature, taking into account the values emitted from the sensors which are located inside that cell
     * - the median temperature, taking into account the values emitted from the sensors which are located inside that cell
     * <p>
     * Q3 output:
     * ts, cell_0, avg_temp0, med_temp0, ... cell_15, avg_temp15, med_temp15
     * <p>
     * Using a tumbling window, calculate this query:
     * – every 1 hour (event time)
     * – every 1 day (event time)
     * – every 1 week (event time)
     *
     * @param args
     */
    public static void main(String[] args) {
        var url = args.length > 0 ? args[0] : "127.0.0.1:29092";
        var q3 = new Query3(url);
        SingleOutputStreamOperator<Query3Record> d = q3.sourceConfigurationAndFiltering();
        var resultStream = q3.queryConfiguration(d, WindowEnum.Hour, "query3-hour"); // TODO: testare
        var resultStream2 = q3.queryConfiguration(d, WindowEnum.Day, "query3-day"); // TODO: testare
        var resultStream3 = q3.queryConfiguration(d, WindowEnum.Week, "query3-week"); // TODO: testare
        q3.sinkConfiguration(resultStream, WindowEnum.Hour);
        q3.sinkConfiguration(resultStream2, WindowEnum.Day);
        q3.sinkConfiguration(resultStream3, WindowEnum.Week);
        q3.execute();
    }

    @Override
    public SingleOutputStreamOperator<Query3Record> sourceConfigurationAndFiltering() {
        var source = KafkaSource.<Query3Record>builder()
                .setBootstrapServers(this.url) // kafka://kafka:9092,
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(QueryRecordDeserializer3.class))
                .build();

        /* TODO: remove this // Checkpointing - Start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/frauddetection/checkpoint");
        */


        var kafkaSource = env.fromSource(source, WatermarkStrategy.<Query3Record>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((query3Record, l) -> query3Record.getTimestamp().getTime()), "Kafka Source")
                .setParallelism(1);
        return kafkaSource.filter(new RecordFilter3());
    }

    @Override
    public SingleOutputStreamOperator<Query3Result> queryConfiguration(SingleOutputStreamOperator<Query3Record> stream, WindowEnum windowAssigner, String opName) {
        return stream.map(new CellMapper()) // we map every value to a Query3Cell. The cell that contains the sensor is calculated inside this mapper
                .filter(a -> a.getCell() != null)// we filter out all records with null cell
                .keyBy(query3Cell -> query3Cell.getCell().getId()) // grouping by id of cell
                .window(windowAssigner.getWindowStrategy()) //setting the desired window strategy
                .aggregate(new AvgMedianAggregate3()) // aggregating averages and medians. This is parallelizable
                .keyBy(CellAvgMedianTemperature::getTimestamp)
                .window(windowAssigner.getWindowStrategy())
                .process(new FinalProcessWindowFunction())
                .map(new MetricRichMapFunction<>())
                .name(opName);
    }

    @Override
    public void sinkConfiguration(SingleOutputStreamOperator<Query3Result> resultStream, WindowEnum windowType) {
        /* Set up the redis sink */
        resultStream.addSink(new RedisHashSink3(windowType));
        /* Set up stdOut Sink */
        resultStream.print();
        if (KAFKA_SINK_ENABLED) {
            /* Set up Kafka sink */
            var sink = KafkaSink.<Query3Result>builder()
                    .setBootstrapServers(url)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("query3-" + windowType.name())
                            .setKafkaValueSerializer(QueryResultSerializer3.class)
                            .build()
                    )
                    .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
            resultStream.sinkTo(sink);
        }
    }
}
