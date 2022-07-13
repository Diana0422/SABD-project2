package com.diagiac.kafka.streams;

import com.diagiac.kafka.bean.SensorDataModel;
import com.diagiac.kafka.streams.bean.AvgResult;
import com.diagiac.kafka.streams.bean.CountAndSum;
import com.diagiac.kafka.streams.metrics.MetricsProcessorSupplier;
import com.diagiac.kafka.streams.serde.AvgCountSerde;
import com.diagiac.kafka.streams.serde.CustomSerde;
import com.diagiac.kafka.streams.serde.WindowSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

public class Query1KafkaStreams {
    public static void main(String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Query1-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka://kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerde.class);
        props.put("metrics.recording.level", "DEBUG");
        props.put("metric.reporters", "org.apache.kafka.common.metrics.JmxReporter");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, SensorDataModel> stream = builder.stream("input-records", Consumed.with(Serdes.Integer(), new CustomSerde())
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
        queryKafka(filtered, windowSizeHour, "query1streams-Hour");

        /* 1 Week window */
        Duration windowSizeWeek = Duration.ofDays(7);
        queryKafka(filtered, windowSizeWeek, "query1streams-Week");

        /* From start window */
        Duration windowSizeMonth = Duration.ofDays(30);
        queryKafka(filtered, windowSizeMonth, "query1streams-FromStart");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public static void queryKafka(KStream<Integer, SensorDataModel> filteredKStream, Duration windowSize, String queryName) {
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(windowSize);

        var kStreamOutput = filteredKStream
                .groupBy((aLong, sensorDataModel) -> Long.valueOf(sensorDataModel.getSensor_id()))
                .windowedBy(tumblingWindow)
                .aggregate(() -> new CountAndSum(0L, 0.0), (aLong, sensorDataModel, countAndSum) -> {
                    countAndSum.setSum(countAndSum.getSum() + Double.parseDouble(sensorDataModel.getTemperature()));
                    countAndSum.setCount(countAndSum.getCount() + 1);
                    return countAndSum;
                }, Materialized.with(Serdes.Long(), new AvgCountSerde()))
                // suppress avoids printing partial results
                .suppress(Suppressed.untilTimeLimit(windowSize, Suppressed.BufferConfig.unbounded()))
                .mapValues((longWindowed, countAndSum) -> new AvgResult(longWindowed.key(), new Timestamp(longWindowed.window().start()), countAndSum.getCount(), countAndSum.getSum() / countAndSum.getCount())
                        .toStringCSV());

        kStreamOutput.toStream().process(new MetricsProcessorSupplier(queryName));
        kStreamOutput.toStream().to(queryName, Produced.with(new WindowSerde(), Serdes.String()));
    }
}
