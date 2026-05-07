# Databricks notebook source
# MAGIC %md
# MAGIC # Research Backtest Example
# MAGIC
# MAGIC Example query path for historical bars, spread features, volatility features, and data-quality annotations.

# COMMAND ----------

import os

catalog = os.getenv("DATABRICKS_CATALOG", "market_data")
schema = os.getenv("DATABRICKS_SCHEMA", "research")
symbol = dbutils.widgets.get("symbol") if "dbutils" in globals() else "AAPL"
start_date = dbutils.widgets.get("start_date") if "dbutils" in globals() else "2026-05-07"
end_date = dbutils.widgets.get("end_date") if "dbutils" in globals() else "2026-05-07"

# COMMAND ----------

bars = spark.table(f"{catalog}.{schema}.gold_bars_1s").where(
    f"symbol = '{symbol}' AND event_date BETWEEN '{start_date}' AND '{end_date}'"
)
spreads = spark.table(f"{catalog}.{schema}.gold_spread_features").where(
    f"symbol = '{symbol}' AND event_date BETWEEN '{start_date}' AND '{end_date}'"
)
quality = spark.table(f"{catalog}.{schema}.gold_quality_annotations").where(
    f"symbol = '{symbol}' AND event_date BETWEEN '{start_date}' AND '{end_date}'"
)

features = (
    bars.alias("b")
    .join(
        spreads.alias("s"),
        ["symbol", "exchange", "event_date", "window_start"],
        "left",
    )
    .select(
        "symbol",
        "exchange",
        "event_date",
        "window_start",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "avg_spread",
        "max_spread",
    )
    .orderBy("window_start")
)

display(features)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

window = Window.partitionBy("symbol", "exchange").orderBy("window_start")
signal_frame = (
    features.withColumn("previous_close", F.lag("close").over(window))
    .withColumn("return_1s", F.when(F.col("previous_close") > 0, F.col("close") / F.col("previous_close") - F.lit(1.0)))
    .withColumn("spread_ok", F.col("avg_spread") <= F.lit(0.05))
    .withColumn("toy_signal", F.when((F.col("return_1s") > 0) & F.col("spread_ok"), F.lit(1)).otherwise(F.lit(0)))
)

display(signal_frame)

# COMMAND ----------

display(
    quality.groupBy("annotation_type", "severity")
    .count()
    .orderBy(F.desc("count"))
)
