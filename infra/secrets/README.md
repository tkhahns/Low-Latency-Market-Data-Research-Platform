# Secrets Management

Production secrets should be stored in GCP Secret Manager and synced into Kubernetes or Cloud Run runtime environment variables by CI/CD.

## Required Secrets

| Secret | Used By | Notes |
| --- | --- | --- |
| `POSTGRES_DSN` | MCP ops, RAG indexer | Cloud SQL PostgreSQL DSN. |
| `RAG_POSTGRES_DSN` | MCP ops server | pgvector database DSN. |
| `DATABRICKS_HOST` | Lakehouse jobs/CI | Databricks workspace URL. |
| `DATABRICKS_TOKEN` | Lakehouse jobs/CI | Databricks service principal token. |
| `PROVIDER_API_KEY` | Future real feed adapter | Not needed for synthetic POC. |
| `OPENAI_API_KEY` | Optional future embedding/agent provider | Not used by the deterministic v1 RAG path. |

## Local Policy

- Do not commit real secrets.
- Use `.env.local` or shell exports for local demos.
- Kubernetes `secret-template.yaml` contains placeholders only.
- Rotate provider keys after demos if shared outside a private environment.
