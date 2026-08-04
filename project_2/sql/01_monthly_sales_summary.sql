-- Part D.1-D.4 -- monthly sales summary.
--
-- `{catalog}` and `{schema}` are substituted by retailpulse/sql_checks.py, so the
-- same file runs locally and on Databricks.  Reference tables as:
--     {catalog}.{schema}.silver_enriched_sales
--
-- Required in this query:
--   D.1  filter to completed orders
--   D.2  join orders, customers and products
--   D.3  use CAST, CASE WHEN, a date function and a string function
--   D.4  group to one row per (order_month, category)
--
-- Emit at least: order_month, category, order_count, total_quantity,
-- total_revenue -- checks/monthly_matches_gold.sql compares these to the Gold
-- table, and Part E builds its events from the same columns.

SELECT 1
