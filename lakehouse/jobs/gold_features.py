from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from common import create_namespace, load_config, register_delta_table


ALERT_SCHEMA = StructType(
    [
        StructField("alert_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("message", StringType(), True),
        StructField("observed_sequence", DoubleType(), True),
        StructField("expected_sequence", DoubleType(), True),
    ]
)


def build_bars(trades):
    return (
        trades.withColumn("window_start", F.date_trunc("second", F.to_timestamp("event_time")))
        .groupBy("symbol", "exchange", "event_date", "window_start")
        .agg(
            F.min_by("price", "event_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_time").alias("close"),
            F.sum("size").alias("volume"),
            (F.sum(F.col("price") * F.col("size")) / F.sum("size")).alias("vwap"),
            F.count("*").alias("trade_count"),
            F.min("sequence_number").alias("first_sequence"),
            F.max("sequence_number").alias("last_sequence"),
            F.min("source_offset").alias("first_source_offset"),
            F.max("source_offset").alias("last_source_offset"),
            F.max("job_run_id").alias("job_run_id"),
            F.max("processed_at").alias("processed_at"),
        )
        .withColumn("schema_version", F.lit("1.0"))
    )


def build_spread_features(quotes):
    return (
        quotes.withColumn("window_start", F.date_trunc("second", F.to_timestamp("event_time")))
        .groupBy("symbol", "exchange", "event_date", "window_start")
        .agg(
            F.count("*").alias("quote_count"),
            F.min("spread").alias("min_spread"),
            F.max("spread").alias("max_spread"),
            F.avg("spread").alias("avg_spread"),
            F.avg("mid_price").alias("avg_mid_price"),
            F.min("sequence_number").alias("first_sequence"),
            F.max("sequence_number").alias("last_sequence"),
            F.min("source_offset").alias("first_source_offset"),
            F.max("source_offset").alias("last_source_offset"),
            F.max("job_run_id").alias("job_run_id"),
            F.max("processed_at").alias("processed_at"),
        )
        .withColumn("schema_version", F.lit("1.0"))
    )


def build_volatility_features(bars):
    window = Window.partitionBy("symbol", "exchange", "event_date").orderBy("window_start")
    returns = bars.withColumn("previous_close", F.lag("close").over(window)).withColumn(
        "log_return",
        F.when((F.col("previous_close") > 0) & (F.col("close") > 0), F.log(F.col("close") / F.col("previous_close"))),
    )
    return (
        returns.groupBy("symbol", "exchange", "event_date")
        .agg(
            F.count("*").alias("bar_count"),
            F.count("log_return").alias("return_count"),
            (F.stddev_samp("log_return") * F.lit(10000.0)).alias("volatility_bps"),
            F.min("window_start").alias("first_window_start"),
            F.max("window_start").alias("last_window_start"),
            F.max("job_run_id").alias("job_run_id"),
            F.max("processed_at").alias("processed_at"),
        )
        .fillna({"volatility_bps": 0.0})
        .withColumn("schema_version", F.lit("1.0"))
    )


def build_quality_annotations(bronze, sequence_gaps):
    alert_events = bronze.where("event_type = 'quality_alert'").withColumn("payload", F.from_json("raw_event", ALERT_SCHEMA))
    alerts = alert_events.select(
        "schema_version",
        "symbol",
        "exchange",
        "event_time",
        "event_date",
        F.col("payload.alert_type").alias("annotation_type"),
        F.col("payload.severity").alias("severity"),
        F.col("payload.message").alias("message"),
        F.lit("bronze_market_events").alias("source_table"),
        "source_topic",
        "source_partition",
        "source_offset",
        "job_run_id",
        "processed_at",
    )
    gaps = sequence_gaps.select(
        "schema_version",
        "symbol",
        "exchange",
        "event_time",
        "event_date",
        F.col("alert_type").alias("annotation_type"),
        F.when(F.col("alert_type") == "sequence_gap", "critical").otherwise("warning").alias("severity"),
        F.concat(
            F.lit("Expected sequence "),
            F.col("expected_sequence").cast("string"),
            F.lit(" but observed "),
            F.col("observed_sequence").cast("string"),
            F.lit("."),
        ).alias("message"),
        F.lit("silver_sequence_gaps").alias("source_table"),
        "source_topic",
        "source_partition",
        "source_offset",
        "job_run_id",
        "processed_at",
    )
    return alerts.unionByName(gaps)


def write_delta(df, path: str, partitions: list[str]) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(*partitions).save(path)


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("gold-features").getOrCreate()
    config = load_config()
    create_namespace(spark, config)

    bronze = spark.read.format("delta").load(config.path("bronze", "bronze_market_events"))
    trades = spark.read.format("delta").load(config.path("silver", "silver_trades"))
    quotes = spark.read.format("delta").load(config.path("silver", "silver_quotes"))
    sequence_gaps = spark.read.format("delta").load(config.path("silver", "silver_sequence_gaps"))

    bars = build_bars(trades)
    outputs = {
        "gold_bars_1s": (bars, ["event_date"]),
        "gold_spread_features": (build_spread_features(quotes), ["event_date"]),
        "gold_volatility_features": (build_volatility_features(bars), ["event_date"]),
        "gold_quality_annotations": (build_quality_annotations(bronze, sequence_gaps), ["event_date"]),
    }
    for table, (df, partitions) in outputs.items():
        path = config.path("gold", table)
        write_delta(df, path, partitions)
        register_delta_table(spark, config, table, path)


if __name__ == "__main__":
    main()
