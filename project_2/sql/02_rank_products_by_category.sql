-- Part D.5 -- rank products by revenue inside each category.
--
-- Use RANK() over a window partitioned by category.  Should agree with
-- gold_top_products_by_category.

WITH product_revenue AS (
    SELECT
        s.category,
        s.product_id,
        s.product_name,
        COUNT(*)              AS order_count,
        SUM(s.quantity)       AS total_quantity,
        SUM(s.net_sales)      AS total_revenue
    FROM {catalog}.{schema}.silver_enriched_sales AS s
    GROUP BY
        s.category,
        s.product_id,
        s.product_name
)
SELECT
    category,
    product_id,
    product_name,
    order_count,
    total_quantity,
    total_revenue,
    RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS revenue_rank
FROM product_revenue
ORDER BY
    category,
    revenue_rank
;