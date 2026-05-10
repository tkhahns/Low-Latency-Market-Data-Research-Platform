# Production Readiness

## What Exists

- CI workflow for Python linting, tests, artifact validation, Docker builds, and Flink Maven packaging.
- Container image definitions for Python services and the Flink job.
- Kubernetes Kustomize manifests with GCP overlay.
- GCP deployment target documentation and Terraform scaffolding.
- Secret Manager-oriented secret policy and Kubernetes secret template.
- OpenTelemetry collector config, structured log contract, Grafana dashboard, and alert rules.
- Runbooks for stale symbols, sequence gaps, Flink failure, Redis divergence, Databricks job failure, and MCP tool failure.
- Backup/recovery guidance for Kafka, Delta, Postgres/pgvector, and Redis.

## Remaining Runtime Gates

- Build and push images in a Docker-enabled environment.
- Package the Java Flink job with Maven.
- Deploy managed Redis, Postgres, Kafka-compatible broker, GKE, and Databricks workspace.
- Run Databricks Asset Bundle validation/deploy.
- Execute throughput and p95/p99 latency benchmarks and record measured values in `docs/performance.md`.

## Demo Checklist

1. Start the local stack or deploy Kubernetes overlay.
2. Confirm synthetic events flow into Kafka-compatible topics.
3. Confirm stream processor writes Redis hot keys.
4. Open dashboard and verify live WebSocket updates.
5. Run API load test and record p95/p99.
6. Run lakehouse jobs or Databricks bundle.
7. Index docs/Obsidian notes into pgvector.
8. Call MCP tools for freshness, sequence gaps, incident summary, and lineage.
