# Databricks notebook source
# Configuration
CATALOG       = "cavallo_test"
BRONZE_TABLE  = f"{CATALOG}.bronze.api_result"
GOLD_TABLE    = f"{CATALOG}.gold.finnhub_stock_company_profile_gold"

# COMMAND ----------

# Read from bronze
bronze_df = spark.table(BRONZE_TABLE)
bronze_df.display()

# COMMAND ----------

# Flatten JSON into typed columns using PySpark
from pyspark.sql.functions import get_json_object, col

gold_df = bronze_df.select(
    col("stock_symbol"),
    get_json_object(col("api_response"), "$.name").alias("company_name"),
    get_json_object(col("api_response"), "$.ticker").alias("ticker"),
    get_json_object(col("api_response"), "$.exchange").alias("exchange"),
    get_json_object(col("api_response"), "$.country").alias("country"),
    get_json_object(col("api_response"), "$.currency").alias("currency"),
    get_json_object(col("api_response"), "$.finnhubIndustry").alias("industry"),
    get_json_object(col("api_response"), "$.ipo").cast("date").alias("ipo_date"),
    get_json_object(col("api_response"), "$.marketCapitalization").cast("double").alias("market_capitalization"),
    get_json_object(col("api_response"), "$.shareOutstanding").cast("double").alias("shares_outstanding"),
    get_json_object(col("api_response"), "$.phone").alias("phone"),
    get_json_object(col("api_response"), "$.weburl").alias("web_url"),
    get_json_object(col("api_response"), "$.logo").alias("logo_url"),
    col("api_call_timestamp")
)

gold_df.display()

# COMMAND ----------

# Write to gold
gold_df.write \
    .mode("overwrite") \
    .saveAsTable(GOLD_TABLE)

print(f"Written {gold_df.count()} rows to {GOLD_TABLE}")
