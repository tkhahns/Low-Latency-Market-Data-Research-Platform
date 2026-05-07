from __future__ import annotations

import os

from common import load_config


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("replay-dry-run").getOrCreate()
    config = load_config()
    replay_date = os.environ["REPLAY_DATE"]
    output_path = os.getenv("REPLAY_OUTPUT_PATH", f"{config.base_path.rstrip('/')}/replay/dry_run/event_date={replay_date}")

    bronze = spark.read.format("delta").load(config.path("bronze", "bronze_market_events")).where(F.col("event_date") == replay_date)
    summary = (
        bronze.groupBy("source_topic", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.min("source_offset").alias("first_offset"),
            F.max("source_offset").alias("last_offset"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
        )
        .withColumn("replay_date", F.lit(replay_date))
        .withColumn("job_run_id", F.lit(config.job_run_id))
        .withColumn("processed_at", F.current_timestamp())
    )
    summary.write.format("delta").mode("overwrite").save(output_path)
    summary.show(truncate=False)


if __name__ == "__main__":
    main()
