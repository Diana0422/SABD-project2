package com.diagiac.flink;

import com.diagiac.flink.bean.SensorRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONObject;
import scala.util.parsing.json.JSON;

public class FlinkConsumer {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.enableCheckpointing(5000);
        // set up the Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // recuperare i dati e pulirli
        var kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        var filtered = kafkaSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = new JSONObject(s);
                return jsonObject.has("temperature") && !jsonObject.getString("temperature").isEmpty();
            }
        });
        var dataStream = filtered.map(new MapFunction<String, SensorRecord>() {
            @Override
            public SensorRecord map(String valueRecord) throws Exception {
                return SensorRecord.create(valueRecord);
            }
        });
        dataStream.executeAndCollect(100).forEach(System.out::println);

        // TODO definire una Watermark Strategy
    }
}
