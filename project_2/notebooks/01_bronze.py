# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Bronze (Part A)
# MAGIC
# MAGIC Reads the three CSVs from `/Volumes/workspace/retail_fresher/retail_raw/`
# MAGIC and writes `bronze_customers`, `bronze_products`, `bronze_sales_orders`
# MAGIC as managed Delta tables, every column still a string, plus `source_file`
# MAGIC and `ingestion_timestamp`.
# MAGIC
# MAGIC The logic lives in `retailpulse/medallion/bronze.py`, the same module
# MAGIC `python -m retailpulse.medallion.bronze` runs locally. This notebook is
# MAGIC only an entry point.

# COMMAND ----------

import logging
import os
import sys

# A Git folder puts only the repo root on sys.path (DBR 11.3+), and the package
# lives one level down in project_2/, this notebook's parent.
sys.path.append(os.path.abspath(".."))

# main() reports row counts through logging; notebooks default to WARNING.
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# COMMAND ----------

from retailpulse.medallion import bronze

# Explicit "databricks": the repo's .env is gitignored and absent here, so
# config.py falls back to its defaults of catalog `workspace`, schema
# `retail_fresher`, volume `retail_raw`.
bronze.main("databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result (Part A.7)
# MAGIC Three Bronze tables. Screenshot this cell for the submission checklist.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.retail_fresher

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers' AS dataset, count(*) AS rows
# MAGIC FROM workspace.retail_fresher.bronze_customers
# MAGIC UNION ALL SELECT 'products', count(*) FROM workspace.retail_fresher.bronze_products
# MAGIC UNION ALL SELECT 'sales_orders', count(*) FROM workspace.retail_fresher.bronze_sales_orders
