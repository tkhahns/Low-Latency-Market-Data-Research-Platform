# OpenTelemetry

OpenTelemetry configuration and instrumentation notes.

Services should emit traces, metrics, and structured logs with shared dimensions:

- `service.name`
- `environment`
- `symbol`
- `exchange`
- `topic`
- `partition`
- `job_name`
- `schema_version`
