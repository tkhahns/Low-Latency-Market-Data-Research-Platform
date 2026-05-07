package com.marketplatform.flink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

final class JsonSerde {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private JsonSerde() {}

  static Map<String, Object> read(String value) throws Exception {
    return MAPPER.readValue(value, MAP_TYPE);
  }

  static String write(Map<String, Object> value) throws Exception {
    return MAPPER.writeValueAsString(value);
  }
}
