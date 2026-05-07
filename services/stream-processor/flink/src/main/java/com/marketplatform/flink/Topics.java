package com.marketplatform.flink;

final class Topics {
  static final String TRADES = "market.trades.v1";
  static final String QUOTES = "market.quotes.v1";
  static final String ALERTS = "market.quality.alerts.v1";
  static final String TOP_OF_BOOK = "market.state.top_of_book.v1";
  static final String BARS_1S = "market.bars.1s.v1";
  static final String ROLLING_METRICS = "market.metrics.rolling.v1";

  private Topics() {}
}
