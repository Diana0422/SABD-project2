package com.flinking_sensors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SimpleWordCount {

    /**
     * Before running this program, open a new socket using the command
     * <p>
     * nc -lk 9999
     * With nc for windows:
     * <p>
     * nc -L -p 9999
     * FIXME: IMPORTANTE per java 17 - usa il comando della JVM:
     *    --add-opens java.base/java.lang=ALL-UNNAMED
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Write something on netcat on another terminal, i will count words ...");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                // If you want to use keyed state, you need to specify a key
                // that should be used to partition the state
                // (and also the records in the stream themselves).
                .keyBy(value -> value.f0)
                .sum(1);

        dataStream.print();

        env.execute("Rolling WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
