package com.diagiac.flink.query3;

import com.diagiac.flink.MetricRichMapFunction;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query3.bean.Query3Record;
import com.diagiac.flink.query3.bean.Query3Result;
import com.diagiac.flink.query3.serialize.QueryRecordDeserializer3;
import com.diagiac.flink.query3.util.AvgMedianAggregate3;
import com.diagiac.flink.query3.util.CellMapper;
import com.diagiac.flink.query3.util.FinalProcessWindowFunction;
import com.diagiac.flink.query3.util.RecordFilter3;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

public class Query3 extends Query<Query3Record, Query3Result> {

    public Query3(String url, WindowEnum w) {
        this.url = url;
        this.windowEnum = w;
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
        var url = args.length > 1 ? args[1] : "127.0.0.1:29092";
        var w = args.length > 0 ? WindowEnum.valueOf(args[0]) : WindowEnum.Hour;
        var q3 = new Query3(url, w);
        SingleOutputStreamOperator<Query3Record> d = q3.sourceConfigurationAndFiltering();
        var resultStream = q3.queryConfiguration(d); // TODO: testare
        q3.sinkConfiguration(resultStream);
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

        /* // Checkpointing - Start a checkpoint every 1000 ms
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
    public SingleOutputStreamOperator<Query3Result> queryConfiguration(SingleOutputStreamOperator<Query3Record> d) {
        return d.map(new CellMapper()) // we map every value to a Query3Cell. The cell that contains the sensor is calculated inside this mapper
                .filter(a -> a.getCell() != null)// we filter out all records with null cell
                .keyBy(query3Cell -> query3Cell.getCell().getId()) // grouping by id of cell
                .window(windowEnum.getWindowStrategy()) //setting the desired window strategy
                .aggregate(new AvgMedianAggregate3()) // aggregating averages and medians. This is parallelizable
                .windowAll(windowEnum.getWindowStrategy()) // this is not parallelizable, but is needed to put all Cell avg/medians in the same window
                .process(new FinalProcessWindowFunction()) // only changes the timestamp to the start of the window!
                .map(new MetricRichMapFunction<>()); // just for metrics
    }

    @Override
    public void sinkConfiguration(SingleOutputStreamOperator<Query3Result> resultStream) {
        /* Set up the redis sink */
        resultStream.addSink(new RedisHashSink3());
        /* Set up stdOut Sink */
        resultStream.print();
    }
}
