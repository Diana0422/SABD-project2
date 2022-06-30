package com.diagiac.flink;

import com.redislabs.redistimeseries.RedisTimeSeries;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.util.Pool;

import java.io.IOException;
import java.util.HashMap;

public class RedisTSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisTSink.class);
    private transient RedisTimeSeries redisTs;
    private RedisTSMapper<IN> redisSinkMapper;
    private String host;
    private int port;

    public RedisTSink(String host, int port, RedisTSMapper<IN> redisSinkMapper) {
        Preconditions.checkNotNull(host, "Redis host should not be null");
        Preconditions.checkNotNull(port, "Redis port should not be null");
        Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.redisTs = new RedisTimeSeries(host, port);
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        String key = redisSinkMapper.getKeyFromData(input);
        String value = redisSinkMapper.getValueFromData(input);
        String ts = redisSinkMapper.getTimestampFromData(input);

        HashMap<String, String> labels = new HashMap<>();
        labels.put(key, value);
        this.redisTs.create(ts, labels);
    }

    @Override
    public void close() throws IOException {
    }
}
