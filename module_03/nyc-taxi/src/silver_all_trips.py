# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as fsum, to_date, lit


spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "new_york_taxi")             
dbutils.widgets.text("silver_schema", "silver")    

CATALOG       = dbutils.widgets.get("catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

# COMMAND ----------

green_silver = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.trips_green")
yellow_silver = spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.trips_yellow")

# COMMAND ----------

all_trips_silver = green_silver.unionByName(yellow_silver)
all_trips_silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.trips_all")
