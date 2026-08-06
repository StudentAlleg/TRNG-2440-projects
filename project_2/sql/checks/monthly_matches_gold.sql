-- Part D.7: reconcile the Spark SQL summary against the PySpark Gold table.
--
-- Contract for everything in sql/checks/: a PASSING check returns ZERO rows.
-- retailpulse/sql_checks.py fails the run if any row comes back.
--
-- A FULL OUTER JOIN between 01_monthly_sales_summary.sql and
-- gold_monthly_category_sales on (order_month, category), returning only rows
-- missing on one side or disagreeing on order_count, total_quantity or
-- total_revenue. Revenue is compared with a tolerance rather than equality,
-- because decimal rounding differs between the two paths.

WITH summary AS (
    SELECT
        date_format(o.order_timestamp, 'yyyy-MM')           AS order_month,
        initcap(p.category)                                 AS category,
        COUNT(*)                                            AS order_count,
        SUM(o.quantity)                                     AS total_quantity,
        SUM(o.quantity * p.unit_price
            - o.quantity * p.unit_price * o.discount_pct / 100) AS total_revenue
    FROM {catalog}.{schema}.silver_sales_orders AS o
    JOIN {catalog}.{schema}.silver_customers   AS c ON c.customer_id = o.customer_id
    JOIN {catalog}.{schema}.silver_products    AS p ON p.product_id  = o.product_id
    WHERE o.order_status NOT IN ('PENDING', 'CANCELLED')
    GROUP BY
        date_format(o.order_timestamp, 'yyyy-MM'),
        initcap(p.category)
)
SELECT
    COALESCE(s.order_month, g.order_month) AS order_month,
    COALESCE(s.category, g.category)       AS category,
    s.order_count                          AS sql_order_count,
    g.order_count                          AS gold_order_count,
    s.total_quantity                       AS sql_total_quantity,
    g.total_quantity                       AS gold_total_quantity,
    s.total_revenue                        AS sql_total_revenue,
    g.total_revenue                        AS gold_total_revenue,
    CASE
        WHEN s.order_month IS NULL THEN 'missing_in_spark_sql'
        WHEN g.order_month IS NULL THEN 'missing_in_gold'
        ELSE 'value_mismatch'
    END                                    AS failure_reason
FROM summary AS s
FULL OUTER JOIN {catalog}.{schema}.gold_monthly_category_sales AS g
    ON  g.order_month = s.order_month
    AND g.category    = s.category
WHERE s.order_month IS NULL
   OR g.order_month IS NULL
   OR s.order_count    <> g.order_count
   OR s.total_quantity <> g.total_quantity
   OR abs(s.total_revenue - g.total_revenue) > 0.01
ORDER BY
    order_month,
    category
;