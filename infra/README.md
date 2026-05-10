# Infrastructure

Infrastructure is split into local development, Kubernetes deployment, cloud provisioning, images, and secrets.

## Paths

| Path | Purpose |
| --- | --- |
| `docker-compose.yml` | Local Redpanda, Redis, Postgres/pgvector, OTel, and app services. |
| `python-service.Dockerfile` | Shared Python service container image. |
| `flink-job.Dockerfile` | Java Flink job container image. |
| `kubernetes` | Kustomize manifests for runtime services. |
| `gcp` | GCP target architecture and deployment flow. |
| `terraform` | GCP managed resource scaffolding. |
| `secrets` | Secret Manager and runtime secret policy. |
| `postgres` | pgvector schema initialization. |
| `images` | Logical image build matrix. |

Production stateful services should be managed outside Kubernetes where possible: Kafka-compatible broker, Redis, Postgres/pgvector, and Databricks.
