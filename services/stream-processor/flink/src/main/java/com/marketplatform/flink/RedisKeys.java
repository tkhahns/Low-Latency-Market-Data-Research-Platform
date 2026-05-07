package com.marketplatform.flink;

final class RedisKeys {
  private RedisKeys() {}

  static String latestQuote(String symbol) {
    return "md:latest_quote:" + symbol.toUpperCase();
  }

  static String topOfBook(String symbol) {
    return "md:top_of_book:" + symbol.toUpperCase();
  }

  static String bar1s(String symbol) {
    return "md:bar:1s:" + symbol.toUpperCase();
  }

  static String freshness(String symbol) {
    return "md:freshness:" + symbol.toUpperCase();
  }

  static String metrics(String symbol) {
    return "md:metrics:" + symbol.toUpperCase();
  }

  static String alerts(String symbol) {
    return "md:alerts:" + symbol.toUpperCase();
  }

  static String activeSymbols() {
    return "md:symbols:active";
  }
}
