# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as fsum, to_date, lit, rank, dense_rank
from pyspark.sql.window import Window


spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "new_york_taxi")
dbutils.widgets.text("gold_schema", "gold")

CATALOG     = dbutils.widgets.get("catalog")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")

# COMMAND ----------

# 1. Read all three gold tables
revenue_by_zone = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.revenue_by_zone")
peak_hours = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.peak_hours")
daily_trends = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.daily_trends")

# COMMAND ----------

# 2. Create zone performance summary with ranking
zone_performance_summary = (
    revenue_by_zone
    .withColumn(
        "revenue_rank",
        rank().over(Window.orderBy(col("total_revenue").desc()))
    )
    .withColumn(
        "trip_rank",
        rank().over(Window.orderBy(col("total_trips").desc()))
    )
    .orderBy(col("revenue_rank"))
)

# COMMAND ----------

zone_performance_summary.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.zone_performance_summary")
