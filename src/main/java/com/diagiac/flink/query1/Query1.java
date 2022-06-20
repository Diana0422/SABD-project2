package com.diagiac.flink.query1;

import com.diagiac.flink.query1.bean.KeyQuery1;
import com.diagiac.flink.query1.bean.Query1Aggregator;
import com.diagiac.flink.query1.bean.QueryRecord1;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.JSONObject;

public class Query1 {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.enableCheckpointing(5000);
        /* set up the Kafka source that consumes records from broker */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("input-records")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        /* clean the data stream ignoring useless information for the query 1
        * - sensor_id < 10.000
        * - temperature != null
        * - temperature > −93.2 °C (lowest temperature ever recorded on earth)
        * - temperature < 56.7 °C (highest temperature ever recorded on earth)
        * */
        var kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        var filtered = kafkaSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                /* Must filter all records that have a valid temperature value and also sensor_id < 10000 as requested.*/
                double temperature;
                long sensorId;
                JSONObject jsonObject = new JSONObject(s);

                boolean temperatureIsPresent = jsonObject.has("temperature")
                        && !jsonObject.getString("temperature").isEmpty();
                boolean sensorIsPresent = jsonObject.has("sensor_id") && !jsonObject.getString("sensor_id").isEmpty();
                if (temperatureIsPresent) {
                    temperature = Double.parseDouble(jsonObject.getString("temperature"));
                } else {
                    return false;
                }
                if (sensorIsPresent) {
                    sensorId = Long.parseLong(jsonObject.getString("sensor_id"));
                } else {
                    return false;
                }
                boolean validTemperature = temperature > -93.2 && temperature < 56.7;
                boolean validSensor = sensorId < 10000;
                return validSensor && validTemperature;
            }
        });
        var dataStream = filtered.map(new MapFunction<String, QueryRecord1>() {
            @Override
            public QueryRecord1 map(String valueRecord) throws Exception {
                return QueryRecord1.create(valueRecord);
            }
        });
        var keyedStream = dataStream.keyBy(new KeySelector<QueryRecord1, KeyQuery1>() {
            @Override
            public KeyQuery1 getKey(QueryRecord1 queryRecord1) throws Exception {
                return KeyQuery1.create(queryRecord1.getTimestamp(), queryRecord1.getSensorId());
            }
        });
        var windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)))
                .allowedLateness(Time.hours(1))
                .aggregate(new AggregateFunction<QueryRecord1, Query1Aggregator, Double>() {
                    @Override
                    public Query1Aggregator createAccumulator() {
                        return new Query1Aggregator(0L, 0L);
                    }

                    @Override
                    public Query1Aggregator add(QueryRecord1 queryRecord1, Query1Aggregator query1Aggregator) {
                        return new Query1Aggregator(
                                queryRecord1.getCount() + query1Aggregator.getCount(),
                                queryRecord1.getTemperature() + query1Aggregator.getTemperatureSum()
                        );
                    }

                    @Override
                    public Double getResult(Query1Aggregator query1Aggregator) {
                        return query1Aggregator.getTemperatureSum() / query1Aggregator.getCount();
                    }

                    @Override
                    public Query1Aggregator merge(Query1Aggregator acc1, Query1Aggregator acc2) {
                        return new Query1Aggregator(
                                acc1.getCount()+acc2.getCount(),
                                acc1.getTemperatureSum()+acc2.getTemperatureSum()
                        );
                    }
                });
        // TODO per effettuare la query è necessario applicare una AggregateFunction sul windowedStream per recuperare i dati non dall'inizio del dataset
        windowedStream.executeAndCollect(5).forEach(System.out::println);

        // TODO definire una Watermark Strategy
    }
}
