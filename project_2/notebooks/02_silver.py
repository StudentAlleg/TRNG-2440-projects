# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Silver (Part B)
# MAGIC
# MAGIC Cleans, casts, deduplicates and enriches Bronze into `silver_customers`,
# MAGIC `silver_products`, `silver_sales_orders` and `silver_enriched_sales`.
# MAGIC
# MAGIC Note `_try_cast` in `silver.py`: Databricks runs with ANSI SQL enabled,
# MAGIC where casting a malformed string ('NA') to a number raises instead of
# MAGIC returning NULL. Local OSS Spark defaults ANSI off. `try_cast` gives both
# MAGIC engines the NULL-then-filter behaviour the requirements describe.

# COMMAND ----------

import logging
import os
import sys

sys.path.append(os.path.abspath(".."))
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# COMMAND ----------

from retailpulse.medallion import silver

silver.main("databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rows in vs rows out (Part B)
# MAGIC Bronze count against Silver count per dataset; the difference is what the
# MAGIC cleaning rules rejected.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers'    AS dataset,
# MAGIC        (SELECT count(*) FROM workspace.retail_fresher.bronze_customers)    AS bronze_rows,
# MAGIC        (SELECT count(*) FROM workspace.retail_fresher.silver_customers)    AS silver_rows
# MAGIC UNION ALL
# MAGIC SELECT 'products',
# MAGIC        (SELECT count(*) FROM workspace.retail_fresher.bronze_products),
# MAGIC        (SELECT count(*) FROM workspace.retail_fresher.silver_products)
# MAGIC UNION ALL
# MAGIC SELECT 'sales_orders',
# MAGIC        (SELECT count(*) FROM workspace.retail_fresher.bronze_sales_orders),
# MAGIC        (SELECT count(*) FROM workspace.retail_fresher.silver_sales_orders)
# MAGIC UNION ALL
# MAGIC SELECT 'enriched_sales', NULL,
# MAGIC        (SELECT count(*) FROM workspace.retail_fresher.silver_enriched_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample of the enriched join (Part B.11, B.12)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, product_id, category, quantity,
# MAGIC        gross_amount, discount_amount, net_amount, profit_per_unit,
# MAGIC        delivery_days, late_delivery_flag, order_month
# MAGIC FROM workspace.retail_fresher.silver_enriched_sales
# MAGIC ORDER BY order_id
# MAGIC LIMIT 20
