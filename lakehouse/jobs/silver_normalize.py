from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

from common import create_namespace, load_config, register_delta_table


TRADE_SCHEMA = StructType(
    [
        StructField("price", DoubleType(), True),
        StructField("size", IntegerType(), True),
        StructField("trade_id", StringType(), True),
        StructField("conditions", ArrayType(StringType()), True),
    ]
)
QUOTE_SCHEMA = StructType(
    [
        StructField("bid_price", DoubleType(), True),
        StructField("bid_size", IntegerType(), True),
        StructField("ask_price", DoubleType(), True),
        StructField("ask_size", IntegerType(), True),
    ]
)


def dedupe(df, keys):
    window = Window.partitionBy(*keys).orderBy(F.col("source_offset").desc())
    return df.withColumn("_row_number", F.row_number().over(window)).where("_row_number = 1").drop("_row_number")


def build_trades(bronze):
    parsed = bronze.where("event_type = 'trade'").withColumn("payload", F.from_json("raw_event", TRADE_SCHEMA))
    return dedupe(
        parsed.select(
            "schema_version",
            "symbol",
            "exchange",
            "event_time",
            "ingest_time",
            "sequence_number",
            "event_date",
            F.coalesce(F.col("payload.trade_id"), F.lit("")).alias("trade_id"),
            F.col("payload.price").alias("price"),
            F.col("payload.size").alias("size"),
            F.coalesce(F.col("payload.conditions"), F.array()).alias("conditions"),
            "source_topic",
            "source_partition",
            "source_offset",
            "job_run_id",
            "processed_at",
        ),
        ["symbol", "exchange", "sequence_number", "trade_id"],
    )


def build_quotes(bronze):
    parsed = bronze.where("event_type = 'quote'").withColumn("payload", F.from_json("raw_event", QUOTE_SCHEMA))
    return dedupe(
        parsed.select(
            "schema_version",
            "symbol",
            "exchange",
            "event_time",
            "ingest_time",
            "sequence_number",
            "event_date",
            F.col("payload.bid_price").alias("bid_price"),
            F.col("payload.bid_size").alias("bid_size"),
            F.col("payload.ask_price").alias("ask_price"),
            F.col("payload.ask_size").alias("ask_size"),
            (F.col("payload.ask_price") - F.col("payload.bid_price")).alias("spread"),
            ((F.col("payload.ask_price") + F.col("payload.bid_price")) / F.lit(2.0)).alias("mid_price"),
            "source_topic",
            "source_partition",
            "source_offset",
            "job_run_id",
            "processed_at",
        ),
        ["symbol", "exchange", "sequence_number"],
    )


def build_sequence_gaps(bronze):
    ordered_window = Window.partitionBy("symbol", "exchange").orderBy("sequence_number", "source_offset")
    previous = F.lag("sequence_number").over(ordered_window)
    return (
        bronze.where("event_type IN ('trade', 'quote')")
        .withColumn("previous_sequence", previous)
        .where("previous_sequence IS NOT NULL AND sequence_number != previous_sequence + 1")
        .select(
            "schema_version",
            "symbol",
            "exchange",
            "event_time",
            "event_date",
            F.when(F.col("sequence_number") <= F.col("previous_sequence"), "duplicate_or_reordered_sequence")
            .otherwise("sequence_gap")
            .alias("alert_type"),
            (F.col("previous_sequence") + F.lit(1)).alias("expected_sequence"),
            F.col("sequence_number").alias("observed_sequence"),
            "source_topic",
            "source_partition",
            "source_offset",
            "job_run_id",
            "processed_at",
        )
    )


def write_delta(df, path: str, partitions: list[str]) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(*partitions).save(path)


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("silver-normalize").getOrCreate()
    config = load_config()
    create_namespace(spark, config)
    bronze = spark.read.format("delta").load(config.path("bronze", "bronze_market_events"))

    outputs = {
        "silver_trades": (build_trades(bronze), ["event_date"]),
        "silver_quotes": (build_quotes(bronze), ["event_date"]),
        "silver_sequence_gaps": (build_sequence_gaps(bronze), ["event_date"]),
    }
    for table, (df, partitions) in outputs.items():
        path = config.path("silver", table)
        write_delta(df, path, partitions)
        register_delta_table(spark, config, table, path)


if __name__ == "__main__":
    main()
