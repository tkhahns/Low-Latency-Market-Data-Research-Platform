# Terraform

Terraform scaffolding for the GCP deployment target. The files in this directory define the intended managed resources but are not applied automatically by local tests.

## Resources

- GKE cluster.
- Artifact Registry repository.
- Memorystore Redis instance.
- Cloud SQL PostgreSQL instance and database.
- Secret Manager placeholders.
- Service account for workloads and CI/CD.

Databricks workspace provisioning is documented in `infra/gcp/README.md`; Databricks resources are deployed through the Asset Bundle in `lakehouse/databricks`.
