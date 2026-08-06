-- Part D.6: top three customers in each state.
--
-- DENSE_RANK() over a window partitioned by state, filtered to rank <= 3.

WITH customer_revenue AS (
    SELECT
        s.state,
        s.customer_id,
        s.customer_name,
        COUNT(*)         AS order_count,
        SUM(s.net_sales) AS lifetime_revenue
    FROM {catalog}.{schema}.silver_enriched_sales AS s
    GROUP BY
        s.state,
        s.customer_id,
        s.customer_name
),
ranked AS (
    SELECT
        customer_revenue.*,
        DENSE_RANK() OVER (PARTITION BY state ORDER BY lifetime_revenue DESC) AS state_rank
    FROM customer_revenue
)
SELECT
    state,
    customer_id,
    customer_name,
    order_count,
    lifetime_revenue,
    state_rank
FROM ranked
WHERE state_rank <= 3
ORDER BY
    state,
    state_rank
;
