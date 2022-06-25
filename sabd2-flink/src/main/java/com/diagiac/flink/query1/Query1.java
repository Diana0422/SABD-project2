package com.diagiac.flink.query1;

import com.diagiac.flink.Query;
import com.diagiac.flink.query1.bean.Query1Record;
import com.diagiac.flink.query1.utils.AverageAggregator;
import com.diagiac.flink.query1.utils.RecordFilter;
import com.diagiac.flink.query1.utils.RecordMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.time.Duration;

public class Query1 extends Query {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.enableCheckpointing(5000);
        /* set up the Kafka source that consumes records from broker */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka://kafka:9092")
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        /* Set up the Redis sink */
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("redis").setPort(6379).build();

        /* clean the data stream ignoring useless information for the query 1
        * - sensor_id < 10.000
        * - temperature != null
        * - temperature > −93.2 °C (lowest temperature ever recorded on earth)
        * - temperature < 56.7 °C (highest temperature ever recorded on earth)
        * */
        var kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        var filtered = kafkaSource.filter(new RecordFilter());
        var dataStream = filtered.map(new RecordMapper()); // FIXME questo map non va bene! è una traformazione che mi fa perdere tempo (cit. Nardelli)
        var water = dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Query1Record>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((queryRecord1, l) -> queryRecord1.getTimestamp().getTime()) // assign the timestamp
        );
        var keyedStream = water.keyBy(Query1Record::getSensorId); // Set the sensorid as the record's key

        /* window hour based */
        var windowedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AverageAggregator());

        keyedStream.print();
        // TODO per effettuare la query è necessario applicare una AggregateFunction sul windowedStream per recuperare i dati non dall'inizio del dataset
        env.execute("Query1");

        // TODO definire una Watermark Strategy
    }

    @Override
    public void initialize() {

    }

    @Override
    public void processing() {

    }

    @Override
    public void preProcessing() {

    }

    @Override
    public void postProcessing() {

    }
}