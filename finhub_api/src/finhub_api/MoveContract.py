# Databricks notebook source
# MAGIC %pip install datacontract-cli pydantic-core typing_extensions==4.12.2 --upgrade --force-reinstall
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datacontract.data_contract import DataContract
import os

contract_path = "/Workspace/Users/karly.kussainova@gmail.com/databricks-onboarding-course/finhub_api/contracts/data_contract.odcs.yml"

# ── Export to HTML ─────────────────────────────────────────────
contract = DataContract(data_contract_file=contract_path)
html_output = contract.export(export_format="html")

html_path = contract_path.replace(".yml", ".html")
with open(html_path, "w") as f:
    f.write(html_output)

print(f"✅ HTML written to: {html_path}")

# ── Copy to Volume ─────────────────────────────────────────────
VOLUME_PATH = "/Volumes/cavallo_test/gold/data_contract/"
dest_path = VOLUME_PATH + "data_contract.html"

with open(html_path, "r", encoding="utf-8") as f:
    content = f.read()

dbutils.fs.put(dest_path, content, overwrite=True)
print(f"✅ Copied to Volume: {dest_path}")
