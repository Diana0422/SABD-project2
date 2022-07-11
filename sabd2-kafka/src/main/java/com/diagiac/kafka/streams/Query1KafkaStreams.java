package com.diagiac.kafka.streams;

import com.diagiac.kafka.bean.SensorDataModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;
import java.util.stream.Stream;

public class Query1KafkaStreams {
    public static void main(String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Query1-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka://kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MySerde.class);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, SensorDataModel> stream = builder.stream("input-records", Consumed.with(Serdes.Integer(), new MySerde())
                .withTimestampExtractor((record, partitionTime) -> {
                    SensorDataModel data = (SensorDataModel) record.value();
                    Timestamp ts = Timestamp.valueOf(data.getTimestamp().replace("T", " "));
                    return ts.getTime();
                }));
        /* filter data input stream */
        var filtered = stream.filter((integer, sensorDataModel) -> {
            String sensorStr = sensorDataModel.getSensor_id();
            String temperatureStr = sensorDataModel.getTemperature();
            String timestampStr = sensorDataModel.getTimestamp();
            boolean presentSensor = sensorStr != null && !sensorStr.equals("");
            boolean presentTimestamp = timestampStr != null && !timestampStr.equals("");
            boolean presentTemperature = temperatureStr != null && !temperatureStr.equals("");
            if (presentTimestamp && presentTemperature && presentSensor) {
                double temperature = Double.parseDouble(temperatureStr);
                long sensorId = Long.parseLong(sensorStr);
                boolean validTemperature = temperature > -93.2 && temperature < 56.7;
                boolean validSensor = sensorId < 10000;
                return validSensor && validTemperature;
            } else {
                return false;
            }
        });



        /* 1 hour window */
        Duration windowSizeHour = Duration.ofMinutes(60);
        TimeWindows tumblingWindowHour = TimeWindows.ofSizeWithNoGrace(windowSizeHour);

        var outputHour = filtered
                .groupBy((aLong, sensorDataModel) -> Long.valueOf(sensorDataModel.getSensor_id()))
                .windowedBy(tumblingWindowHour)
                .aggregate(() -> new CountAndSum(0L, 0.0), (aLong, sensorDataModel, countAndSum) -> {
                    countAndSum.setSum(countAndSum.getSum() + Double.parseDouble(sensorDataModel.getTemperature()));
                    countAndSum.setCount(countAndSum.getCount() + 1);
                    return countAndSum;
                }, Materialized.with(Serdes.Long(), new AvgCountSerde()))
                .suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(60), Suppressed.BufferConfig.unbounded()))
                .mapValues((longWindowed, countAndSum) -> new AvgResult(longWindowed.key(), new Timestamp(longWindowed.window().start()), countAndSum.getCount(), countAndSum.getSum()/ countAndSum.getCount()).toStringCSV());


        outputHour.toStream().to("query1streams-Hour", Produced.with(new WindowSerde(), Serdes.String()));

        /* 1 Week window */
        Duration windowSizeWeek = Duration.ofDays(7);
        TimeWindows tumblingWindowWeek = TimeWindows.ofSizeWithNoGrace(windowSizeWeek);

        var outputWeek = filtered
                .groupBy((aLong, sensorDataModel) -> Long.valueOf(sensorDataModel.getSensor_id()))
                .windowedBy(tumblingWindowWeek)
                .aggregate(() -> new CountAndSum(0L, 0.0), (aLong, sensorDataModel, countAndSum) -> {
                    System.out.println("sensorDataModel = " + sensorDataModel);
                    countAndSum.setSum(countAndSum.getSum() + Double.parseDouble(sensorDataModel.getTemperature()));
                    countAndSum.setCount(countAndSum.getCount() + 1);
                    return countAndSum;
                }, Materialized.with(Serdes.Long(), new AvgCountSerde()))
                .suppress(Suppressed.untilTimeLimit(Duration.ofDays(7), Suppressed.BufferConfig.unbounded()))
                .mapValues((longWindowed, countAndSum) -> new AvgResult(longWindowed.key(), new Timestamp(longWindowed.window().start()), countAndSum.getCount(), countAndSum.getSum()/ countAndSum.getCount()).toStringCSV());


        outputWeek.toStream().to("query1streams-Week", Produced.with(new WindowSerde(), Serdes.String()));

        /* From start window */
        Duration windowSizeMonth = Duration.ofDays(30);
        TimeWindows tumblingWindowMonth = TimeWindows.ofSizeWithNoGrace(windowSizeMonth);

        var outputStart = filtered
                .groupBy((aLong, sensorDataModel) -> Long.valueOf(sensorDataModel.getSensor_id()))
                .windowedBy(tumblingWindowMonth)
                .aggregate(() -> new CountAndSum(0L, 0.0), (aLong, sensorDataModel, countAndSum) -> {
                    countAndSum.setSum(countAndSum.getSum() + Double.parseDouble(sensorDataModel.getTemperature()));
                    countAndSum.setCount(countAndSum.getCount() + 1);
                    return countAndSum;
                }, Materialized.with(Serdes.Long(), new AvgCountSerde()))
                .suppress(Suppressed.untilTimeLimit(Duration.ofDays(30), Suppressed.BufferConfig.unbounded()))
                .mapValues((longWindowed, countAndSum) -> new AvgResult(longWindowed.key(), new Timestamp(longWindowed.window().start()), countAndSum.getCount(), countAndSum.getSum()/ countAndSum.getCount()).toStringCSV());


        outputStart.toStream().to("query1streams-FromStart", Produced.with(new WindowSerde(), Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
