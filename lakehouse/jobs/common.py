from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseConfig:
    base_path: str
    checkpoint_path: str
    catalog: str
    schema: str
    job_run_id: str

    def path(self, zone: str, table: str) -> str:
        return f"{self.base_path.rstrip('/')}/{zone}/{table}"

    def checkpoint(self, job_name: str) -> str:
        return f"{self.checkpoint_path.rstrip('/')}/{job_name}"

    def table_name(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"


def load_config() -> LakehouseConfig:
    return LakehouseConfig(
        base_path=os.getenv("LAKEHOUSE_BASE_PATH", "dbfs:/tmp/low_latency_market_data_platform/delta"),
        checkpoint_path=os.getenv("LAKEHOUSE_CHECKPOINT_PATH", "dbfs:/tmp/low_latency_market_data_platform/checkpoints"),
        catalog=os.getenv("DATABRICKS_CATALOG", "market_data"),
        schema=os.getenv("DATABRICKS_SCHEMA", "research"),
        job_run_id=os.getenv("DATABRICKS_JOB_RUN_ID", "manual"),
    )


def create_namespace(spark, config: LakehouseConfig) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema}")


def register_delta_table(spark, config: LakehouseConfig, table: str, path: str) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {config.table_name(table)} USING DELTA LOCATION '{path}'")
