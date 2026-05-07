from __future__ import annotations

import os

from common import load_config


TABLES = [
    ("bronze", "bronze_market_events"),
    ("silver", "silver_trades"),
    ("silver", "silver_quotes"),
    ("silver", "silver_sequence_gaps"),
    ("gold", "gold_bars_1s"),
    ("gold", "gold_spread_features"),
    ("gold", "gold_volatility_features"),
    ("gold", "gold_quality_annotations"),
]


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("quality-report").getOrCreate()
    config = load_config()
    report_date = os.getenv("REPORT_DATE")
    output_path = os.getenv("QUALITY_REPORT_PATH", f"{config.base_path.rstrip('/')}/reports/quality")

    reports = []
    for zone, table in TABLES:
        df = spark.read.format("delta").load(config.path(zone, table))
        if report_date and "event_date" in df.columns:
            df = df.where(F.col("event_date") == report_date)
        reports.append(
            df.agg(
                F.count("*").alias("row_count"),
                F.min("event_date").alias("min_event_date") if "event_date" in df.columns else F.lit(None).alias("min_event_date"),
                F.max("event_date").alias("max_event_date") if "event_date" in df.columns else F.lit(None).alias("max_event_date"),
            )
            .withColumn("zone", F.lit(zone))
            .withColumn("table_name", F.lit(table))
            .withColumn("report_date", F.lit(report_date or "all"))
            .withColumn("job_run_id", F.lit(config.job_run_id))
            .withColumn("processed_at", F.current_timestamp())
        )
    report = reports[0]
    for frame in reports[1:]:
        report = report.unionByName(frame)
    report.write.format("delta").mode("append").save(output_path)
    report.show(truncate=False)


if __name__ == "__main__":
    main()
