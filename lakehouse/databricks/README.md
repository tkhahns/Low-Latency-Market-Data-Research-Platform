# Databricks

Databricks Asset Bundle assets for the cold path.

## Bundle

The bundle file is `bundle.yml` and defines dev/prod targets plus jobs for:

- `llmdp_bronze_ingest`
- `llmdp_silver_normalize`
- `llmdp_gold_features`
- `llmdp_quality_report`
- `llmdp_replay_dry_run`

Deploy from this directory when the Databricks CLI is available:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

The local environment used in this repo does not require Databricks dependencies in `.venv`; job code is syntax-checked locally and runs with Spark on Databricks.

Placeholder for Databricks Asset Bundles, job definitions, notebooks, and cluster policies.

## Planned Assets

- Bronze ingestion job.
- Silver normalization job.
- Gold feature generation job.
- Replay dry-run job.
- Data quality checks.

Implementation should use Databricks Jobs or Workflows for lakehouse orchestration in v1.
