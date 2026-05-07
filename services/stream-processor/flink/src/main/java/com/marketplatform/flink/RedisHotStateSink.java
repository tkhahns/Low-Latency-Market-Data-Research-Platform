package com.marketplatform.flink;

import java.net.URI;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisPooled;

public class RedisHotStateSink extends RichSinkFunction<String> {
  private final String redisUrl;
  private transient JedisPooled jedis;

  public RedisHotStateSink(String redisUrl) {
    this.redisUrl = redisUrl;
  }

  @Override
  public void open(Configuration parameters) {
    jedis = new JedisPooled(URI.create(redisUrl));
  }

  @Override
  public void invoke(String value, Context context) throws Exception {
    Map<String, Object> event = JsonSerde.read(value);
    String symbol = EventTransforms.string(event, "symbol");
    String eventType = EventTransforms.string(event, "event_type");
    jedis.sadd(RedisKeys.activeSymbols(), symbol);

    if ("quote".equals(eventType)) {
      jedis.setex(RedisKeys.latestQuote(symbol), 120, value);
    } else if ("top_of_book".equals(eventType)) {
      jedis.setex(RedisKeys.topOfBook(symbol), 120, value);
      jedis.setex(RedisKeys.freshness(symbol), 120, JsonSerde.write(EventTransforms.freshness(event)));
    } else if ("bar_1s".equals(eventType)) {
      jedis.setex(RedisKeys.bar1s(symbol), 120, value);
    } else if ("rolling_metrics".equals(eventType)) {
      jedis.setex(RedisKeys.metrics(symbol), 120, value);
    } else if ("quality_alert".equals(eventType)) {
      jedis.lpush(RedisKeys.alerts(symbol), value);
      jedis.ltrim(RedisKeys.alerts(symbol), 0, 24);
      jedis.expire(RedisKeys.alerts(symbol), 3600);
    }
  }

  @Override
  public void close() {
    if (jedis != null) {
      jedis.close();
    }
  }
}
