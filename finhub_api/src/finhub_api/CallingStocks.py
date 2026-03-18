# Databricks notebook source
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ExternalFunctionRequestHttpMethod
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import json
from datetime import datetime

# COMMAND ----------

w = WorkspaceClient()

# Configuration — all parameters in one place
CONNECTION_NAME = "test_api"
API_TOKEN = "d6ssfehr01qoqoirbdg0d6ssfehr01qoqoirbdgg"
STOCKS = ["RY", "TD", "BMO", "BNS", "CM"]

# COMMAND ----------

def get_stock_profile(symbol: str) -> dict:
    """
    Calls Finnhub for a single stock symbol.
    Returns a dict with the symbol, raw JSON response, and timestamp.
    Bronze layer keeps the response raw — no parsing yet.
    """
    try:
        response = w.serving_endpoints.http_request(
            conn=CONNECTION_NAME,
            method=ExternalFunctionRequestHttpMethod.GET,
            path="/stock/profile2",
            params={"symbol": symbol, "token": API_TOKEN}
        )
        return {
            "stock_symbol":        symbol,
            "api_response":        response.text,
            "api_call_timestamp":  datetime.now()
        }
    except Exception as e:
        print(f"Failed for {symbol}: {e}")
        return None

# COMMAND ----------

# Test it with just one stock first
result = get_stock_profile("RY")
print(result)

# COMMAND ----------

successful = []
failed     = []

for stock in STOCKS:
    print(f"Calling API for {stock}...")
    result = get_stock_profile(stock)
    if result:
        successful.append(result)
        print(f"  ✓ {stock}")
    else:
        failed.append(stock)
        print(f"  ✗ {stock}")

print(f"\nDone. {len(successful)} succeeded, {len(failed)} failed.")
if failed:
    print(f"Failed symbols: {failed}")


# COMMAND ----------

schema = StructType([
    StructField("stock_symbol",        StringType(),    True),
    StructField("api_response",        StringType(),    True),
    StructField("api_call_timestamp",  TimestampType(), True),
])

if successful:
    df = spark.createDataFrame(successful, schema)
    df.display() 
    
    df.write \
      .mode("append") \
      .saveAsTable("cavallo_test.bronze.api_result")
