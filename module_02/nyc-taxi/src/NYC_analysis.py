# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as fsum, to_date, lit


spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading and Writitng the raw tables 

# COMMAND ----------

# Fixed names for Module 2

CATALOG = "new_york_taxi"
VOLUME = "raw_files"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD   = f"{CATALOG}.{GOLD_SCHEMA}"

# COMMAND ----------

# 1. Ensure output schemas exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

print("Catalog and schemas ensured:")
print("-", BRONZE)
print("-", SILVER)
print("-", GOLD)

# COMMAND ----------

# 2. Read raw tables from the volumes
base_path = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}"

green_path  = f"{base_path}/green_tripdata_2024-01.parquet"
yellow_path = f"{base_path}/yellow_tripdata_2024-01.parquet"
zones_path  = f"{base_path}/taxi_zone_lookup.csv"

green_raw = spark.read.parquet(green_path)
yellow_raw = spark.read.parquet(yellow_path)
zones_raw = spark.read.option("header", "true").csv(zones_path)

print("Files read from Volume:")
print("-", green_path)
print("-", yellow_path)
print("-", zones_path)

# COMMAND ----------

# DBTITLE 1,Untitled
# 3. Write Bronze managed Delta tables
green_raw.write.mode("overwrite").saveAsTable(f"{BRONZE}.green_tripdata")
yellow_raw.write.mode("overwrite").saveAsTable(f"{BRONZE}.yellow_tripdata")
zones_raw.write.mode("overwrite").saveAsTable(f"{BRONZE}.taxi_zone_lookup")

print("Bronze tables written:")
print("-", f"{BRONZE}.green_tripdata")
print("-", f"{BRONZE}.yellow_tripdata")
print("-", f"{BRONZE}.taxi_zone_lookup")

# COMMAND ----------

# 4. Read Bronze for transformations

green = spark.table(f"{BRONZE}.green_tripdata")
yellow = spark.table(f"{BRONZE}.yellow_tripdata")
zones = spark.table(f"{BRONZE}.taxi_zone_lookup")

print("Bronze row counts:")
print("green:", green.count())
print("yellow:", yellow.count())
print("zones:", zones.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysing the table schemas and writing them in the SILVER

# COMMAND ----------

# 1. Analyse the schemas
print("green columns:", green.columns)
print("yellow columns:", yellow.columns)
print("zones columns:", zones.columns)

# COMMAND ----------

# 2. Standartize the green trips
green_silver = (
    green_raw
    .select(
        lit("green").alias("service_type"),
        col("VendorID").cast("string").alias("vendor_id"),
        col("lpep_pickup_datetime").alias("pickup_ts"),
        col("lpep_dropoff_datetime").alias("dropoff_ts"),
        col("PULocationID").cast("int").alias("pickup_location_id"),
        col("DOLocationID").cast("int").alias("dropoff_location_id"),
        col("passenger_count").cast("int").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("tip_amount").cast("double").alias("tip_amount"),
        col("total_amount").cast("double").alias("total_amount"),
        col("payment_type").cast("string").alias("payment_type")
    )
    .filter(col("pickup_ts").isNotNull())
    .filter(col("dropoff_ts").isNotNull())
    .filter(col("trip_distance") >= 0)
    .withColumn("trip_date", to_date(col("pickup_ts")))
)


# COMMAND ----------

green_silver.display()

# COMMAND ----------

# 3. Standartize the yellow trips
yellow_silver = (
    yellow_raw
    .select(
        lit("yellow").alias("service_type"),
        col("VendorID").alias("vendor_id"),
        col("tpep_pickup_datetime").alias("pickup_ts"),
        col("tpep_dropoff_datetime").alias("dropoff_ts"),
        lit(None).cast("int").alias("pickup_location_id"),   # not present in this file
        lit(None).cast("int").alias("dropoff_location_id"),  # not present in this file
        col("Passenger_Count").cast("int").alias("passenger_count"),
        col("Trip_Distance").cast("double").alias("trip_distance"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("tip_amount").cast("double").alias("tip_amount"),
        col("total_amount").cast("double").alias("total_amount"),
        col("payment_type").cast("string").alias("payment_type")
    )
    .filter(col("pickup_ts").isNotNull())
    .filter(col("dropoff_ts").isNotNull())
    .filter(col("trip_distance") >= 0)
    .withColumn("trip_date", to_date(col("pickup_ts")))
)


# COMMAND ----------

yellow_silver.display()

# COMMAND ----------

# 4. Standartize the zones
zones_silver = (
    zones_raw
    .select(
        col("LocationID").cast("int").alias("location_id"),
        col("Borough").cast("string").alias("borough"),
        col("Zone").cast("string").alias("zone"),
        col("service_zone").cast("string").alias("service_zone")
    )
)

# COMMAND ----------

zones_silver.display()

# COMMAND ----------

# 5. Write SILVER
green_silver.write.mode("overwrite").saveAsTable(f"{SILVER}.trips_green_clean")
yellow_silver.write.mode("overwrite").saveAsTable(f"{SILVER}.trips_yellow_clean")
zones_silver.write.mode("overwrite").saveAsTable(f"{SILVER}.dim_taxi_zone")

all_trips_silver = green_silver.unionByName(yellow_silver)
all_trips_silver.write.mode("overwrite").saveAsTable(f"{SILVER}.trips_all_clean")

print("Silver tables created:")
print("-", f"{SILVER}.trips_green_clean")
print("-", f"{SILVER}.trips_yellow_clean")
print("-", f"{SILVER}.trips_all_clean")
print("-", f"{SILVER}.dim_taxi_zone")
