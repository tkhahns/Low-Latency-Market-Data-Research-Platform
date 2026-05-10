# Container Images

All Python services use `infra/python-service.Dockerfile`; the Flink job uses `infra/flink-job.Dockerfile`.

| Logical Image | Dockerfile | Runtime Command |
| --- | --- | --- |
| `feed-simulator` | `infra/python-service.Dockerfile` | `python -m market_platform.services.feed_simulator` |
| `feed-handler` | `infra/python-service.Dockerfile` | `python -m market_platform.services.feed_handler` |
| `stream-processor` | `infra/python-service.Dockerfile` | `python -m market_platform.services.stream_processor` |
| `market-data-api` | `infra/python-service.Dockerfile` | `python -m market_platform.services.market_data_api` |
| `mcp-ops-server` | `infra/python-service.Dockerfile` | `python -m market_platform.services.mcp_ops_server` |
| `flink-market-state-job` | `infra/flink-job.Dockerfile` | `com.marketplatform.flink.MarketStateJob` |

Example build:

```bash
docker build -f infra/python-service.Dockerfile -t us-docker.pkg.dev/PROJECT_ID/market-data/market-data-api:latest .
docker build -f infra/flink-job.Dockerfile -t us-docker.pkg.dev/PROJECT_ID/market-data/flink-market-state-job:latest .
```
