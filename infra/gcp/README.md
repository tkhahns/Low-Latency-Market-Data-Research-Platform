# GCP Deployment Target

The preferred production target is GCP with managed stateful services and Kubernetes workloads.

## Target Services

| Capability | GCP-Oriented Choice |
| --- | --- |
| Container runtime | GKE Autopilot for long-running API and processor services. |
| Kafka-compatible broker | Redpanda Cloud, Confluent Cloud, or self-managed Redpanda on GKE for demos. |
| Redis hot cache | Memorystore for Redis. |
| PostgreSQL + pgvector | Cloud SQL for PostgreSQL with pgvector extension enabled. |
| Delta Lake | Databricks on GCP. |
| Secrets | Secret Manager. |
| Images | Artifact Registry. |
| Metrics/logs | Cloud Monitoring plus Grafana dashboards. |

## Deployment Flow

1. Build and push images to Artifact Registry.
2. Provision managed Redis, Postgres, broker, and Databricks workspace.
3. Apply Kubernetes manifests from `infra/kubernetes/overlays/gcp`.
4. Deploy Databricks Asset Bundle from `lakehouse/databricks`.
5. Index repo docs and Obsidian vault notes into pgvector.
6. Run benchmark scripts and record results in `docs/performance.md`.
