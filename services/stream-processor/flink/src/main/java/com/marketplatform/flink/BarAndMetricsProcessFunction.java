package com.marketplatform.flink;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BarAndMetricsProcessFunction extends KeyedProcessFunction<String, String, String> {
  private transient ValueState<BarState> barState;
  private transient ValueState<RollingState> rollingState;

  @Override
  public void open(Configuration parameters) {
    barState = getRuntimeContext().getState(new ValueStateDescriptor<>("bar-1s", BarState.class));
    rollingState = getRuntimeContext().getState(new ValueStateDescriptor<>("rolling-metrics", RollingState.class));
  }

  @Override
  public void processElement(String value, Context context, Collector<String> out) throws Exception {
    Map<String, Object> trade = JsonSerde.read(value);
    double price = EventTransforms.number(trade, "price");
    int size = EventTransforms.integer(trade, "size");
    String windowStart = Instant.parse(EventTransforms.string(trade, "event_time")).truncatedTo(ChronoUnit.SECONDS).toString();

    BarState bar = barState.value();
    if (bar == null || !bar.windowStart.equals(windowStart)) {
      bar = new BarState(windowStart, price, price, price, price, size, price * size);
    } else {
      bar.update(price, size);
    }
    barState.update(bar);
    out.collect(JsonSerde.write(bar.toEvent(trade)));

    RollingState rolling = rollingState.value();
    if (rolling == null) {
      rolling = new RollingState();
    }
    rolling.add(EventTransforms.string(trade, "event_time"), price, size);
    rollingState.update(rolling);
    out.collect(JsonSerde.write(rolling.toEvent(trade)));
  }

  public static class BarState {
    public String windowStart;
    public double open;
    public double high;
    public double low;
    public double close;
    public int volume;
    public double notional;

    public BarState() {}

    BarState(String windowStart, double open, double high, double low, double close, int volume, double notional) {
      this.windowStart = windowStart;
      this.open = open;
      this.high = high;
      this.low = low;
      this.close = close;
      this.volume = volume;
      this.notional = notional;
    }

    void update(double price, int size) {
      high = Math.max(high, price);
      low = Math.min(low, price);
      close = price;
      volume += size;
      notional += price * size;
    }

    Map<String, Object> toEvent(Map<String, Object> trade) {
      Map<String, Object> out = EventTransforms.base(trade, "bar_1s");
      out.put("event_time", windowStart);
      out.put("ingest_time", EventTransforms.now());
      out.put("open", open);
      out.put("high", high);
      out.put("low", low);
      out.put("close", close);
      out.put("volume", volume);
      out.put("vwap", notional / volume);
      return out;
    }
  }

  public static class RollingState {
    public List<Double> prices = new ArrayList<>();
    public int volume = 0;
    public double notional = 0.0;

    public void add(String eventTime, double price, int size) {
      prices.add(price);
      if (prices.size() > 120) {
        prices = new ArrayList<>(prices.subList(prices.size() - 120, prices.size()));
      }
      volume += size;
      notional += price * size;
    }

    Map<String, Object> toEvent(Map<String, Object> trade) {
      List<Double> returns = new ArrayList<>();
      for (int i = 1; i < prices.size(); i++) {
        double previous = prices.get(i - 1);
        double current = prices.get(i);
        if (previous > 0.0 && current > 0.0) {
          returns.add(Math.log(current / previous));
        }
      }
      double volatility = 0.0;
      if (returns.size() > 1) {
        double mean = returns.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = returns.stream().mapToDouble(value -> Math.pow(value - mean, 2)).sum() / (returns.size() - 1);
        volatility = Math.sqrt(variance) * 10000.0;
      }
      Map<String, Object> out = EventTransforms.base(trade, "rolling_metrics");
      out.put("ingest_time", EventTransforms.now());
      out.put("sample_count", prices.size());
      out.put("rolling_volume", volume);
      out.put("rolling_vwap", notional / volume);
      out.put("volatility_bps", EventTransforms.round6(volatility));
      return out;
    }
  }
}
