-- Part D.7 -- reconcile the Spark SQL summary against the PySpark Gold table.
--
-- Contract for everything in sql/checks/: a PASSING check returns ZERO rows.
-- retailpulse/sql_checks.py fails the run if any row comes back.
--
-- Build this as a FULL OUTER JOIN between 01_monthly_sales_summary.sql and
-- {catalog}.{schema}.gold_monthly_category_sales on (order_month, category),
-- returning only rows that are missing on one side or disagree on
-- order_count / total_quantity / total_revenue.  Compare the revenue with a
-- small tolerance rather than equality -- decimal rounding differs between the
-- two paths.

SELECT 1 WHERE false
