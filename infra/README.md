# Infrastructure

Infrastructure is split into local development, Kubernetes deployment, and cloud provisioning.

## Local Dependencies

The first local stack should include:

- Kafka-compatible broker.
- Redis.
- PostgreSQL with pgvector.
- OpenTelemetry collector.
- Grafana or a metrics backend.

Application containers should be added after service runtimes are selected.
