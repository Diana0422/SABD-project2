package com.diagiac.flink.query2;

import com.diagiac.flink.MetricRichMapFunction;
import com.diagiac.flink.Query;
import com.diagiac.flink.WindowEnum;
import com.diagiac.flink.query2.bean.Query2Record;
import com.diagiac.flink.query2.bean.Query2Result;
import com.diagiac.flink.query2.serialize.QueryRecordDeserializer2;
import com.diagiac.flink.query2.util.AverageAggregator2;
import com.diagiac.flink.query2.util.RecordFilter2;
import com.diagiac.flink.query2.util.SortKeyedProcessFunction;
import com.diagiac.flink.redis.TheRedisMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.time.Duration;

public class Query2 extends Query<Query2Record,Query2Result> {

    public Query2(String url, WindowEnum w) {
        this.url = url;
        this.windowEnum = w;
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
        var window = args.length > 0 ? WindowEnum.valueOf(args[0]) : WindowEnum.Hour;
        var url = args.length > 1 ? args[1] : "127.0.0.1:29092";
        var q2 = new Query2(url, window);
        SingleOutputStreamOperator<Query2Record> d =  q2.sourceConfigurationAndFiltering();
        var resultStream = q2.queryConfiguration(d); // TODO: testare
        q2.sinkConfiguration(resultStream);
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
    public SingleOutputStreamOperator<Query2Result> queryConfiguration(SingleOutputStreamOperator<Query2Record> d) {
        // Query2Record -> (Location, resto di query2Record)
        var locationKeyed = d.keyBy(Query2Record::getLocation);
        var windowed = locationKeyed.window(windowEnum.getWindowStrategy());

        // (Location, resto di query2Record) -> (Location, avgTemperature) nella finestra
        var aggregated = windowed.aggregate(new AverageAggregator2());
        var windowedAll = aggregated.windowAll(windowEnum.getWindowStrategy());
        var processed = windowedAll.process(new SortKeyedProcessFunction());
        processed.map(new MetricRichMapFunction<>()); // just for metrics
        return processed;
    }

    @Override
    public void sinkConfiguration(SingleOutputStreamOperator<Query2Result> resultStream) {
        /* Set up the redis sink */
        var conf = new FlinkJedisPoolConfig.Builder().setHost("redis-cache").setPort(6379).build();
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "timestamp", "getTimestamp")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location1", "getLocation1")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location2", "getLocation2")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location3", "getLocation3")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location4", "getLocation4")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location5", "getLocation5")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location6", "getLocation6")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location7", "getLocation7")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location8", "getLocation8")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location9", "getLocation9")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "location10", "getLocation10")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature1", "getTemperature1")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature2", "getTemperature2")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature3", "getTemperature3")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature4", "getTemperature4")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature5", "getTemperature5")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature6", "getTemperature6")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature7", "getTemperature7")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature8", "getTemperature8")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature9", "getTemperature9")));
        resultStream.addSink(new RedisSink<>(conf, new TheRedisMapper<>("query2", "temperature10", "getTemperature10")));
        /* Set up stdOut Sink */
        resultStream.print();
    }
}
