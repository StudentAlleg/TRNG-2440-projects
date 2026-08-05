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

SELECT
    date_format(o.order_timestamp, 'yyyy-MM')                                   AS order_month,
    initcap(p.category)                                                         AS category,
    COUNT(*)                                                                    AS order_count,
    COUNT(DISTINCT c.customer_id)                                               AS customer_count,
    SUM(o.quantity)                                                             AS total_quantity,
    SUM(o.quantity * p.unit_price
        - o.quantity * p.unit_price * o.discount_pct / 100)                     AS total_revenue,
    CAST(AVG(o.quantity * p.unit_price
        - o.quantity * p.unit_price * o.discount_pct / 100) AS DECIMAL(18, 2))  AS avg_order_value,
    SUM(CASE WHEN o.actual_delivery_date > o.promised_delivery_date
             THEN 1 ELSE 0 END)                                                 AS late_delivery_count
FROM {catalog}.{schema}.silver_sales_orders AS o
JOIN {catalog}.{schema}.silver_customers   AS c ON c.customer_id = o.customer_id
JOIN {catalog}.{schema}.silver_products    AS p ON p.product_id  = o.product_id
WHERE o.order_status NOT IN ('PENDING', 'CANCELLED')
GROUP BY
    date_format(o.order_timestamp, 'yyyy-MM'),
    initcap(p.category)
ORDER BY
    order_month,
    category
;
