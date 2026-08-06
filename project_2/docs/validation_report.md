# RetailPulse data-quality and control-total report

Counts from a local-mode run over `spark_catalog.retail_fresher`.

## 1. Input: raw CSV vs Bronze

| Dataset | CSV records | Bronze rows |
|---|---:|---:|
| `customers_500.csv` | 500 | 500 |
| `products_500.csv` | 500 | 500 |
| `sales_orders_500.csv` | 500 | 500 |

## 2. Rejected rows: the Bronze to Silver funnel

Rules apply in the order `medallion/silver.py` applies them, so a row failing
two rules counts only against the first one it hits.

### `customers`

Bronze rows in: **500**, Silver rows out: **461**, rejected: **39** (7.8%)

| Req | Rule | Rejected | Surviving |
|---|---|---:|---:|
| B.5 | superseded profile versions (row_number over updated_at desc) | 10 | 490 |
| B.6 | inactive customers (is_active <> 'Y') | 29 | 461 |

### `products`

Bronze rows in: **500**, Silver rows out: **469**, rejected: **31** (6.2%)

| Req | Rule | Rejected | Surviving |
|---|---|---:|---:|
| B.7 | unit_price / cost_price missing or not numeric | 10 | 490 |
| B.7 | price <= 0, cost <= 0, or cost > price | 0 | 490 |
| B.8 | discontinued products (active_flag <> 'Y') | 21 | 469 |

### `sales_orders`

Bronze rows in: **500**, Silver rows out: **429**, rejected: **71** (14.2%)

| Req | Rule | Rejected | Surviving |
|---|---|---:|---:|
| B.9 | quantity null, zero or negative | 11 | 489 |
| B.10 | order_status in (PENDING, CANCELLED) | 60 | 429 |

## 3. Processed rows: the join (B.12)

An order can clear every Silver rule and still be lost at the join when the
product or customer it points at was itself rejected.

| Step | Rows |
|---|---:|
| silver_sales_orders rows entering the join | 429 |
| dropped, product_id absent from silver_products | 28 |
| dropped, customer_id absent from silver_customers | 24 |
| silver_enriched_sales rows | 377 |

## 4. Gold tables

| Table | Rows |
|---|---:|
| `gold_monthly_category_sales` | 30 |
| `gold_city_sales` | 17 |
| `gold_customer_value` | 371 |
| `gold_top_products_by_category` | 377 |

## 5. Control totals

Revenue is the same number in Silver, in Gold and in the published events.

| Measure | Value | Check |
|---|---:|:--:|
| silver_enriched_sales rows | 377 | |
| gold_monthly_category_sales SUM(order_count) | 377 | PASS |
| silver_enriched_sales SUM(quantity) | 1,167 | |
| gold_monthly_category_sales SUM(total_quantity) | 1,167 | PASS |
| silver_enriched_sales SUM(net_sales) | 4,447,062.53 | |
| gold_monthly_category_sales SUM(total_revenue) | 4,447,062.53 | PASS |
| Part E events published | 30 | PASS |
| Part E events SUM(total_revenue) | 4,447,062.53 | PASS |

`sql/checks/monthly_matches_gold.sql` adds a sixth comparison: the Spark SQL
monthly summary against `gold_monthly_category_sales`, full outer joined. It
returns zero rows. Run it with `python -m retailpulse.sql_checks`.
