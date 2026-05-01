# Kafka Topics

| Topic | Producer | Primary Consumers | Retention Intent |
| --- | --- | --- | --- |
| `market.raw.v1` | Feed handler | Bronze writer, diagnostics | Durable replay window. |
| `market.trades.v1` | Feed handler | Flink, bronze writer | Durable replay window. |
| `market.quotes.v1` | Feed handler | Flink, bronze writer | Durable replay window. |
| `market.state.top_of_book.v1` | Flink | API, lakehouse | Short to medium retention. |
| `market.bars.1s.v1` | Flink | API, lakehouse | Medium retention. |
| `market.quality.alerts.v1` | Feed handler, Flink | API, MCP, observability | Medium retention. |

Topic names include a major schema version. Minor schema evolution should remain backward compatible.
