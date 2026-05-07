from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from common import create_namespace, load_config, register_delta_table


EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("exchange", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("ingest_time", StringType(), False),
        StructField("sequence_number", IntegerType(), False),
    ]
)


def bronze_frame(kafka_df, job_run_id: str):
    value = F.col("value").cast("string")
    parsed = F.from_json(value, EVENT_SCHEMA)
    return (
        kafka_df.select(
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            value.alias("raw_event"),
            parsed.alias("event"),
        )
        .select(
            F.col("event.schema_version").alias("schema_version"),
            F.col("event.event_type").alias("event_type"),
            F.upper(F.col("event.symbol")).alias("symbol"),
            F.col("event.exchange").alias("exchange"),
            F.col("event.event_time").alias("event_time"),
            F.col("event.ingest_time").alias("ingest_time"),
            F.col("event.sequence_number").alias("sequence_number"),
            F.to_date(F.to_timestamp("event.event_time")).alias("event_date"),
            "source_topic",
            "source_partition",
            "source_offset",
            F.lit(job_run_id).alias("job_run_id"),
            F.current_timestamp().alias("processed_at"),
            "raw_event",
        )
        .where("schema_version IS NOT NULL AND event_type IS NOT NULL")
    )


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("bronze-ingest").getOrCreate()
    config = load_config()
    create_namespace(spark, config)

    topics = ",".join(
        [
            "market.raw.v1",
            "market.trades.v1",
            "market.quotes.v1",
            "market.state.top_of_book.v1",
            "market.bars.1s.v1",
            "market.metrics.rolling.v1",
            "market.quality.alerts.v1",
        ]
    )
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", __import__("os").getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"))
        .option("subscribe", topics)
        .option("startingOffsets", __import__("os").getenv("KAFKA_STARTING_OFFSETS", "earliest"))
        .load()
    )
    path = config.path("bronze", "bronze_market_events")
    query = (
        bronze_frame(kafka, config.job_run_id)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint("bronze_ingest"))
        .partitionBy("event_date", "event_type")
        .trigger(availableNow=True)
        .start(path)
    )
    query.awaitTermination()
    register_delta_table(spark, config, "bronze_market_events", path)


if __name__ == "__main__":
    main()
