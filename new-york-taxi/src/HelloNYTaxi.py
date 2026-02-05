# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "new_york_taxi"
SCHEMA = "bronze"
VOLUME = "raw_files"

# COMMAND ----------

# 1) Create catalog + schemas (safe to run multiple times)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

base_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# 2) Read the physical files from the Volume
green_df = spark.read.parquet(f"{base_path}/green_tripdata_2015-05.parquet")
yellow_df = spark.read.parquet(f"{base_path}/yellow_tripdata_2009-01.parquet")
zones_df = spark.read.option("header", "true").csv(f"{base_path}/taxi_zone_lookup.csv")


# COMMAND ----------

# 3) Show that it worked
print("✅ Hello, New-York Taxi! ")

green_df.display()
