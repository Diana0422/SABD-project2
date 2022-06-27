package com.diagiac.flink.query3;

import com.diagiac.flink.FlinkRecord;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query3.bean.Query3Cell;
import com.diagiac.flink.query3.bean.Query3Record;
import com.diagiac.flink.query3.util.CellMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

public class Query3 extends Query {

    public Query3(String url) {
        this.url = url;
    }


    /**
     * Consider the latitude and longitude coordinates within the geographic area
     * which is identified from the latitude and longitude coordinates (38°, 2°) and (58°, 30°).
     *
     * Divide this area using a 4x4 grid and identify each grid cell from the top-left to bottom-right corners
     * using the name "cell_X", where X is the cell id from 0 to 15. For each cell, find
     *
     * - the average temperature, taking into account the values emitted from the sensors which are located inside that cell
     * - the median temperature, taking into account the values emitted from the sensors which are located inside that cell
     *
     * Q3 output:
     * ts, cell_0, avg_temp0, med_temp0, ... cell_15, avg_temp15, med_temp15
     *
     * Using a tumbling window, calculate this query:
     * – every 1 hour (event time)
     * – every 1 day (event time)
     * – every 1 week (event time)
     * @param args
     */
    public static void main(String[] args) {
        var url = args.length > 1 ? args[1] : "127.0.0.1:29092";
        var q3 = new Query3(url);
        SingleOutputStreamOperator<Query3Record> d =  q3.initialize();
        q3.queryConfiguration(d, args.length > 0 ? WindowEnum.valueOf(args[0]) : WindowEnum.Hour); // TODO: testare
        q3.sinkConfiguration();
        q3.execute();
    }

    @Override
    public SingleOutputStreamOperator<Query3Record> initialize() {
        var source = KafkaSource.<Query3Record>builder()
                .setBootstrapServers(this.url) // kafka://kafka:9092,
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(QueryRecordDeserializer3.class))
                .build();

        /* // Checkpointing - Start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/frauddetection/checkpoint");
        */

        var kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        return kafkaSource.filter(new RecordFilter3());
    }

    @Override
    public void queryConfiguration(SingleOutputStreamOperator<? extends FlinkRecord> d, WindowEnum window) {
        var dd = (SingleOutputStreamOperator<Query3Record>) d;
        var water = dd.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Query3Record>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((queryRecord3, l) -> queryRecord3.getTimestamp().getTime()) // assign the timestamp
        );

        // Query2Record -> (Location, resto di query2Record)


        var mapped = water.map(new CellMapper());
        var filtered = mapped.filter(a -> a.getCell() != null);
        var keyed = mapped.keyBy(Query3Cell::getCell);
        var windowed = keyed.window(window.getWindowStrategy());
        var aggregated = windowed.aggregate(new AvgMedianAggregator3());

    }

    @Override
    public void sinkConfiguration() {

    }
}
