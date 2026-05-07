package com.marketplatform.flink;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

final class EventTransforms {
  static final long STALE_MS = 2_000L;

  private EventTransforms() {}

  static Map<String, Object> topOfBook(Map<String, Object> quote) {
    double bid = number(quote, "bid_price");
    double ask = number(quote, "ask_price");
    double spread = round6(ask - bid);
    String processedTime = now();
    Map<String, Object> out = base(quote, "top_of_book");
    out.put("processed_time", processedTime);
    out.put("bid_price", bid);
    out.put("bid_size", integer(quote, "bid_size"));
    out.put("ask_price", ask);
    out.put("ask_size", integer(quote, "ask_size"));
    out.put("spread", spread);
    out.put("mid_price", round6((ask + bid) / 2.0));
    return out;
  }

  static Map<String, Object> freshness(Map<String, Object> quote) {
    long lagMs = Math.max(0L, Duration.between(Instant.parse(string(quote, "event_time")), Instant.now()).toMillis());
    Map<String, Object> out = new HashMap<>();
    out.put("schema_version", "1.0");
    out.put("symbol", string(quote, "symbol"));
    out.put("exchange", string(quote, "exchange"));
    out.put("event_time", string(quote, "event_time"));
    out.put("last_ingest_time", string(quote, "ingest_time"));
    out.put("last_processed_time", now());
    out.put("freshness_lag_ms", lagMs);
    out.put("status", lagMs > STALE_MS ? "stale" : "fresh");
    return out;
  }

  static Map<String, Object> qualityAlert(Map<String, Object> event, String type, String severity, String message) {
    Map<String, Object> out = new HashMap<>();
    out.put("schema_version", "1.0");
    out.put("event_type", "quality_alert");
    out.put("symbol", string(event, "symbol"));
    out.put("exchange", string(event, "exchange"));
    out.put("event_time", now());
    out.put("ingest_time", now());
    out.put("sequence_number", integer(event, "sequence_number"));
    out.put("alert_type", type);
    out.put("severity", severity);
    out.put("message", message);
    out.put("observed_sequence", integer(event, "sequence_number"));
    out.put("expected_sequence", null);
    return out;
  }

  static Map<String, Object> base(Map<String, Object> event, String eventType) {
    Map<String, Object> out = new HashMap<>();
    out.put("schema_version", "1.0");
    out.put("event_type", eventType);
    out.put("symbol", string(event, "symbol"));
    out.put("exchange", string(event, "exchange"));
    out.put("event_time", string(event, "event_time"));
    out.put("ingest_time", string(event, "ingest_time"));
    out.put("sequence_number", integer(event, "sequence_number"));
    return out;
  }

  static String now() {
    return Instant.now().toString();
  }

  static String string(Map<String, Object> value, String key) {
    return String.valueOf(value.get(key));
  }

  static double number(Map<String, Object> value, String key) {
    return ((Number) value.get(key)).doubleValue();
  }

  static int integer(Map<String, Object> value, String key) {
    return ((Number) value.get(key)).intValue();
  }

  static double round6(double value) {
    return Math.round(value * 1_000_000.0) / 1_000_000.0;
  }
}
