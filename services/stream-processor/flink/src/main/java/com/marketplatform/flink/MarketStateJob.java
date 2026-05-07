package com.marketplatform.flink;

import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MarketStateJob {
  public static void main(String[] args) throws Exception {
    String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    String redisUrl = env("REDIS_URL", "redis://localhost:6379/0");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(60_000L);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
    env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

    DataStream<String> quotes = env.fromSource(
        source(bootstrapServers, Topics.QUOTES, "flink-market-state-quotes"),
        WatermarkStrategy.noWatermarks(),
        "quotes");
    DataStream<String> trades = env.fromSource(
        source(bootstrapServers, Topics.TRADES, "flink-market-state-trades"),
        WatermarkStrategy.noWatermarks(),
        "trades");
    DataStream<String> alerts = env.fromSource(
        source(bootstrapServers, Topics.ALERTS, "flink-market-state-alerts"),
        WatermarkStrategy.noWatermarks(),
        "alerts");

    DataStream<String> topOfBook = quotes.map(value -> JsonSerde.write(EventTransforms.topOfBook(JsonSerde.read(value))));
    DataStream<String> quoteAlerts = topOfBook.flatMap((String value, org.apache.flink.util.Collector<String> out) -> {
      Map<String, Object> top = JsonSerde.read(value);
      double spread = EventTransforms.number(top, "spread");
      double mid = EventTransforms.number(top, "mid_price");
      if (spread < 0.0) {
        out.collect(JsonSerde.write(EventTransforms.qualityAlert(top, "crossed_market", "critical", "Ask price is below bid price.")));
      } else if (spread > Math.max(0.05, mid * 0.002)) {
        out.collect(JsonSerde.write(EventTransforms.qualityAlert(top, "wide_spread", "warning", "Spread exceeded configured threshold.")));
      }
    }).returns(String.class);

    DataStream<String> barsAndMetrics = trades
        .keyBy(value -> EventTransforms.string(JsonSerde.read(value), "symbol"))
        .process(new BarAndMetricsProcessFunction());

    DataStream<String> bars = barsAndMetrics.filter(value -> "bar_1s".equals(EventTransforms.string(JsonSerde.read(value), "event_type")));
    DataStream<String> rollingMetrics = barsAndMetrics.filter(value -> "rolling_metrics".equals(EventTransforms.string(JsonSerde.read(value), "event_type")));
    DataStream<String> allAlerts = alerts.union(quoteAlerts);

    topOfBook.sinkTo(sink(bootstrapServers, Topics.TOP_OF_BOOK)).name("top-of-book-topic");
    bars.sinkTo(sink(bootstrapServers, Topics.BARS_1S)).name("bars-1s-topic");
    rollingMetrics.sinkTo(sink(bootstrapServers, Topics.ROLLING_METRICS)).name("rolling-metrics-topic");
    quoteAlerts.sinkTo(sink(bootstrapServers, Topics.ALERTS)).name("quality-alerts-topic");

    quotes.addSink(new RedisHotStateSink(redisUrl)).name("redis-latest-quotes");
    topOfBook.addSink(new RedisHotStateSink(redisUrl)).name("redis-top-of-book");
    bars.addSink(new RedisHotStateSink(redisUrl)).name("redis-bars");
    rollingMetrics.addSink(new RedisHotStateSink(redisUrl)).name("redis-metrics");
    allAlerts.addSink(new RedisHotStateSink(redisUrl)).name("redis-alerts");

    env.execute("market-state-flink-job");
  }

  private static KafkaSource<String> source(String bootstrapServers, String topic, String groupId) {
    Properties properties = new Properties();
    properties.setProperty("isolation.level", "read_committed");
    return KafkaSource.<String>builder()
        .setBootstrapServers(bootstrapServers)
        .setTopics(topic)
        .setGroupId(groupId)
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setProperties(properties)
        .build();
  }

  private static KafkaSink<String> sink(String bootstrapServers, String topic) {
    return KafkaSink.<String>builder()
        .setBootstrapServers(bootstrapServers)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic(topic)
            .setValueSerializationSchema(new SimpleStringSchema())
            .build())
        .build();
  }

  private static String env(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }
}
