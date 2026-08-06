# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Gold (Part B.14 and Part C)
# MAGIC
# MAGIC Builds the four analytical tables, including the window functions:
# MAGIC `row_number()` for the latest customer profile, `rank()` for top products
# MAGIC per category, `dense_rank()` for customers within a state, and a running
# MAGIC revenue total by category and month.
# MAGIC
# MAGIC `gold_monthly_category_sales` is the table Part E's Kafka events are
# MAGIC built from, so it must keep `order_month`, `category`, `order_count`,
# MAGIC `total_quantity` and `total_revenue`.

# COMMAND ----------

import logging
import os
import sys

sys.path.append(os.path.abspath(".."))
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# COMMAND ----------

from retailpulse.medallion import gold

gold.main("databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly category sales and running total (C.1, C.2, C.6)
# MAGIC These rows are what the Kafka producer publishes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_month, category, order_count, total_quantity, total_revenue,
# MAGIC        avg_order_value, min_order_value, max_order_value, running_total_revenue
# MAGIC FROM workspace.retail_fresher.gold_monthly_category_sales
# MAGIC ORDER BY category, order_month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top products by category, C.4 (`rank()`)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category, product_name, total_quantity, total_revenue, revenue_rank
# MAGIC FROM workspace.retail_fresher.gold_top_products_by_category
# MAGIC WHERE revenue_rank <= 3
# MAGIC ORDER BY category, revenue_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers within each state, C.5 (`dense_rank()`) and C.7 (latest order)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT state, customer_id, lifetime_revenue, latest_order, state_rank
# MAGIC FROM workspace.retail_fresher.gold_customer_value
# MAGIC WHERE state_rank <= 3
# MAGIC ORDER BY state, state_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ## City sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, state, order_count, total_revenue
# MAGIC FROM workspace.retail_fresher.gold_city_sales
# MAGIC ORDER BY total_revenue DESC
