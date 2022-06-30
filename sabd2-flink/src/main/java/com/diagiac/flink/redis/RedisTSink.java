package com.diagiac.flink.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;

public class RedisTSink<IN> extends RichSinkFunction<IN> {

    //private static final Logger LOG = LoggerFactory.getLogger(RedisTSink.class);
    // private transient RedisTimeSeries redisTs;
    private final RedisTSMapper<IN> redisSinkMapper;
    private final String host;
    private final int port;

    public RedisTSink(String host, int port, RedisTSMapper<IN> redisSinkMapper) {
        Preconditions.checkNotNull(host, "Redis host should not be null");
        Preconditions.checkNotNull(port, "Redis port should not be null");
        Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");
        this.host = host;
        this.port = port;
        this.redisSinkMapper = redisSinkMapper;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // this.redisTs = new RedisTimeSeries(host, port);
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        String key = redisSinkMapper.getKeyFromData(input);
        String value = redisSinkMapper.getValueFromData(input);
        String ts = redisSinkMapper.getTimestampFromData(input);

        HashMap<String, String> labels = new HashMap<>();
        labels.put(key, value);
        // this.redisTs.create(ts, labels);
    }

    @Override
    public void close() throws IOException {
    }
}
