---
title: Databricks Job Failure Runbook
tags: [runbook, databricks, delta, lakehouse]
---

# Databricks Job Failure Runbook

Symptoms:
- Bronze, silver, gold, replay, or quality report jobs fail.
- Delta tables stop receiving new partitions.
- Research queries miss expected event dates.

Checks:
1. Inspect the failed Databricks job run and cluster logs.
2. Confirm source Kafka offsets or bronze input partitions exist.
3. Validate schema compatibility against `contracts/events` and `lakehouse/contracts/tables.yml`.
4. Check storage permissions for `LAKEHOUSE_BASE_PATH` and checkpoint paths.
5. Review recent bundle deployments.

Recovery:
- Rerun the failed job with the same job run parameters.
- For bronze streaming jobs, verify checkpoint consistency before deleting checkpoints.
- For silver/gold overwrite jobs, rerun from bronze for the affected date.
- Run `quality_report.py` after recovery and attach the report to the incident.
