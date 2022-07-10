package com.diagiac.kafka.streams;

import com.diagiac.kafka.bean.SensorDataModel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

public class Query1KafkaStreams {
    public static void main(String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Query1-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka://kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> stream = builder.stream("input-records");
        System.out.println("recovered data");
        var mapped = stream.mapValues((key, value) -> SensorDataModel.create(value));
        /* filter data input stream */
        var filtered = mapped.filter((integer, sensorDataModel) -> {
            System.out.println("INPUT DATA: "+sensorDataModel.toString());
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
        System.out.println("filtered data");
        /* 1 hour window */
        Duration windowSizeHour = Duration.ofMinutes(60);
        TimeWindows tumblingWindowHour = TimeWindows.ofSizeWithNoGrace(windowSizeHour);

        var output = filtered.groupBy((aLong, sensorDataModel) -> Long.valueOf(sensorDataModel.getSensor_id()))
                .windowedBy(tumblingWindowHour)
                .aggregate(() -> new CountAndSum(null,0L, 0.0), (aLong, sensorDataModel, countAndSum) -> {
                    countAndSum.setSum(countAndSum.getSum() + Double.parseDouble(sensorDataModel.getTemperature()));
                    countAndSum.setCount(countAndSum.getCount() + 1);
                    return countAndSum;
                })
                .mapValues((longWindowed, countAndSum) -> new AvgResult(longWindowed.key(), new Timestamp(longWindowed.window().start()), countAndSum.getCount(), countAndSum.getSum()/ countAndSum.getCount()).toStringCSV());


        output.toStream().to("query1streams-Hour", Produced.with(new WindowedSerdes.TimeWindowedSerde<Long>(), Serdes.String()));

        /* 1 Week window */
        Duration windowSizeWeek = Duration.ofDays(7);
        TimeWindows tumblingWindowWeek = TimeWindows.ofSizeWithNoGrace(windowSizeWeek);

        /* From start window */
        Duration windowSizeMonth = Duration.ofDays(30);
        TimeWindows tumblingWindowMonth = TimeWindows.ofSizeWithNoGrace(windowSizeMonth);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
