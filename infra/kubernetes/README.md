# Kubernetes

Kustomize manifests for production-like deployment.

## Layout

| Path | Purpose |
| --- | --- |
| `base` | Namespace, ConfigMap, Secret template, Deployments, and Services. |
| `overlays/gcp` | GCP-oriented managed-service configuration patch. |

## Runtime Services

- `feed-simulator`
- `feed-handler`
- `stream-processor`
- `market-data-api`
- `mcp-ops-server`

Stateful services are expected to be managed outside the cluster in production:

- Kafka-compatible broker: Redpanda Cloud, Confluent Cloud, or managed Kafka.
- Redis: Memorystore for Redis.
- PostgreSQL + pgvector: Cloud SQL for PostgreSQL with pgvector support.
- Delta Lake: Databricks on GCP.

## Deploy

```bash
kubectl apply -k infra/kubernetes/overlays/gcp
```

Before deployment, replace `PROJECT_ID`, image tags, broker endpoint, Redis endpoint, and secret values through CI/CD or Secret Manager sync.
