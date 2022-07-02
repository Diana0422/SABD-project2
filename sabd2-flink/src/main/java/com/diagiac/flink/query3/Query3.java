package com.diagiac.flink.query3;

import com.diagiac.flink.MetricRichMapFunction;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query3.bean.Query3Record;
import com.diagiac.flink.query3.bean.Query3Result;
import com.diagiac.flink.query3.serialize.QueryRecordDeserializer3;
import com.diagiac.flink.query3.util.AvgMedianAggregator3;
import com.diagiac.flink.query3.util.CellMapper;
import com.diagiac.flink.query3.util.FinalProcessWindowFunction;
import com.diagiac.flink.query3.util.RecordFilter3;
import com.diagiac.flink.redis.TheRedisMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.time.Duration;

public class Query3 extends Query<Query3Record,Query3Result> {

    public Query3(String url, WindowEnum w) {
        this.url = url;
        this.windowEnum = w;
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
        var w = args.length > 0 ? WindowEnum.valueOf(args[0]) : WindowEnum.Hour;
        var q3 = new Query3(url, w);
        SingleOutputStreamOperator<Query3Record> d =  q3.sourceConfigurationAndFiltering();
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
        var mapped = d.map(new CellMapper());
        var filtered = mapped.filter(a -> a.getCell() != null);
        var keyed = filtered.keyBy(query3Cell -> query3Cell.getCell().getId());
        var windowed = keyed.window(windowEnum.getWindowStrategy());
        var aggregated = windowed.aggregate(new AvgMedianAggregator3());
        var windowedAll = aggregated.windowAll(windowEnum.getWindowStrategy());
        var finalProcess = windowedAll.process(new FinalProcessWindowFunction());
        finalProcess.map(new MetricRichMapFunction<>()); // just for metrics
        return finalProcess;
    }

    @Override
    public void sinkConfiguration(SingleOutputStreamOperator<Query3Result> resultStream) {
        /* Set up the redis sink */
        var conf = new FlinkJedisPoolConfig.Builder().setHost("redis-cache").setPort(6379).build();
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "timestamp", "getTimestamp")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp0", "getAvgTemp0")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp1", "getAvgTemp1")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp2", "getAvgTemp2")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp3", "getAvgTemp3")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp4", "getAvgTemp4")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp5", "getAvgTemp5")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp6", "getAvgTemp6")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp7", "getAvgTemp7")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp8", "getAvgTemp8")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp9", "getAvgTemp9")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp10", "getAvgTemp10")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp11", "getAvgTemp11")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp12", "getAvgTemp12")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp13", "getAvgTemp13")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp14", "getAvgTemp14")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "avgTemp15", "getAvgTemp15")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp0", "getMedianTemp0")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp1", "getMedianTemp1")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp2", "getMedianTemp2")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp3", "getMedianTemp3")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp4", "getMedianTemp4")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp5", "getMedianTemp5")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp6", "getMedianTemp6")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp7", "getMedianTemp7")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp8", "getMedianTemp8")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp9", "getMedianTemp9")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp10", "getMedianTemp10")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp11", "getMedianTemp11")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp12", "getMedianTemp12")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp13", "getMedianTemp13")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp14", "getMedianTemp14")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query3", "medianTemp15", "getMedianTemp15")));
        /* Set up stdOut Sink */
        resultStream.print();
    }
}
