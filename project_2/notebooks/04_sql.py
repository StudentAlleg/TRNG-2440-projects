# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse: Spark SQL (Part D)
# MAGIC
# MAGIC Reproduces the PySpark analysis in pure Spark SQL against the managed
# MAGIC Delta tables, then reconciles the two paths:
# MAGIC
# MAGIC | Req | Query |
# MAGIC |---|---|
# MAGIC | D.1 to D.4 | `sql/01_monthly_sales_summary.sql`, three-way join, monthly summary |
# MAGIC | D.5 | `sql/02_rank_products_by_category.sql`, `RANK()` by category revenue |
# MAGIC | D.6 | `sql/03_top_customers_per_state.sql`, `DENSE_RANK()`, top three per state |
# MAGIC | D.7 | `sql/checks/monthly_matches_gold.sql`, Spark SQL against the Gold table |
# MAGIC
# MAGIC The `.sql` files are the source of truth; the cells below inline them so
# MAGIC the results render in the notebook for the submission screenshots.

# COMMAND ----------

import logging
import os
import sys

sys.path.append(os.path.abspath(".."))
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run every script and every reconciliation check
# MAGIC `sql_checks.main` executes each file in `sql/` and then each file in
# MAGIC `sql/checks/`, raising `SystemExit` if any check returns a row. A clean
# MAGIC run ends with `all reconciliation checks passed`.

# COMMAND ----------

from retailpulse import sql_checks

sql_checks.main("databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.1 to D.4: monthly sales summary
# MAGIC Completed orders only, joined across all three datasets, with `CAST`,
# MAGIC `CASE WHEN`, `date_format()` and `initcap()`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     date_format(o.order_timestamp, 'yyyy-MM')            AS order_month,
# MAGIC     initcap(p.category)                                  AS category,
# MAGIC     COUNT(*)                                             AS order_count,
# MAGIC     COUNT(DISTINCT c.customer_id)                        AS customer_count,
# MAGIC     SUM(o.quantity)                                      AS total_quantity,
# MAGIC     SUM(o.quantity * p.unit_price
# MAGIC         - o.quantity * p.unit_price * o.discount_pct / 100) AS total_revenue,
# MAGIC     CAST(AVG(o.quantity * p.unit_price
# MAGIC         - o.quantity * p.unit_price * o.discount_pct / 100)
# MAGIC          AS DECIMAL(18, 2))                              AS avg_order_value,
# MAGIC     SUM(CASE WHEN o.actual_delivery_date > o.promised_delivery_date
# MAGIC              THEN 1 ELSE 0 END)                          AS late_delivery_count
# MAGIC FROM workspace.retail_fresher.silver_sales_orders AS o
# MAGIC JOIN workspace.retail_fresher.silver_customers   AS c ON c.customer_id = o.customer_id
# MAGIC JOIN workspace.retail_fresher.silver_products    AS p ON p.product_id  = o.product_id
# MAGIC WHERE o.order_status NOT IN ('PENDING', 'CANCELLED')
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY order_month, category

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.5: rank products by category revenue (`RANK()`)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH product_revenue AS (
# MAGIC     SELECT
# MAGIC         s.category,
# MAGIC         s.product_id,
# MAGIC         s.product_name,
# MAGIC         COUNT(*)         AS order_count,
# MAGIC         SUM(s.quantity)  AS total_quantity,
# MAGIC         SUM(s.net_sales) AS total_revenue
# MAGIC     FROM workspace.retail_fresher.silver_enriched_sales AS s
# MAGIC     GROUP BY s.category, s.product_id, s.product_name
# MAGIC )
# MAGIC SELECT
# MAGIC     category, product_id, product_name, order_count, total_quantity, total_revenue,
# MAGIC     RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS revenue_rank
# MAGIC FROM product_revenue
# MAGIC QUALIFY revenue_rank <= 5
# MAGIC ORDER BY category, revenue_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.6: top three customers in each state (`DENSE_RANK()`)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_revenue AS (
# MAGIC     SELECT
# MAGIC         s.state,
# MAGIC         s.customer_id,
# MAGIC         s.customer_name,
# MAGIC         COUNT(*)         AS order_count,
# MAGIC         SUM(s.net_sales) AS lifetime_revenue
# MAGIC     FROM workspace.retail_fresher.silver_enriched_sales AS s
# MAGIC     GROUP BY s.state, s.customer_id, s.customer_name
# MAGIC )
# MAGIC SELECT
# MAGIC     state, customer_id, customer_name, order_count, lifetime_revenue,
# MAGIC     DENSE_RANK() OVER (PARTITION BY state ORDER BY lifetime_revenue DESC) AS state_rank
# MAGIC FROM customer_revenue
# MAGIC QUALIFY state_rank <= 3
# MAGIC ORDER BY state, state_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ## D.7: compare the Spark SQL results with the PySpark results
# MAGIC A full outer join between the SQL summary and `gold_monthly_category_sales`.
# MAGIC **Zero rows means the two paths agree.** That empty result is the screenshot
# MAGIC to keep.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH summary AS (
# MAGIC     SELECT
# MAGIC         date_format(o.order_timestamp, 'yyyy-MM')            AS order_month,
# MAGIC         initcap(p.category)                                  AS category,
# MAGIC         COUNT(*)                                             AS order_count,
# MAGIC         SUM(o.quantity)                                      AS total_quantity,
# MAGIC         SUM(o.quantity * p.unit_price
# MAGIC             - o.quantity * p.unit_price * o.discount_pct / 100) AS total_revenue
# MAGIC     FROM workspace.retail_fresher.silver_sales_orders AS o
# MAGIC     JOIN workspace.retail_fresher.silver_customers   AS c ON c.customer_id = o.customer_id
# MAGIC     JOIN workspace.retail_fresher.silver_products    AS p ON p.product_id  = o.product_id
# MAGIC     WHERE o.order_status NOT IN ('PENDING', 'CANCELLED')
# MAGIC     GROUP BY 1, 2
# MAGIC )
# MAGIC SELECT
# MAGIC     COALESCE(s.order_month, g.order_month) AS order_month,
# MAGIC     COALESCE(s.category, g.category)       AS category,
# MAGIC     s.order_count    AS sql_order_count,   g.order_count    AS gold_order_count,
# MAGIC     s.total_quantity AS sql_total_quantity, g.total_quantity AS gold_total_quantity,
# MAGIC     s.total_revenue  AS sql_total_revenue,  g.total_revenue  AS gold_total_revenue,
# MAGIC     CASE
# MAGIC         WHEN s.order_month IS NULL THEN 'missing_in_spark_sql'
# MAGIC         WHEN g.order_month IS NULL THEN 'missing_in_gold'
# MAGIC         ELSE 'value_mismatch'
# MAGIC     END AS failure_reason
# MAGIC FROM summary AS s
# MAGIC FULL OUTER JOIN workspace.retail_fresher.gold_monthly_category_sales AS g
# MAGIC     ON g.order_month = s.order_month AND g.category = s.category
# MAGIC WHERE s.order_month IS NULL
# MAGIC    OR g.order_month IS NULL
# MAGIC    OR s.order_count    <> g.order_count
# MAGIC    OR s.total_quantity <> g.total_quantity
# MAGIC    OR abs(s.total_revenue - g.total_revenue) > 0.01
# MAGIC ORDER BY order_month, category
