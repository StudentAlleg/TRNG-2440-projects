"""Parts B.14 and C -- Gold analytical tables and window functions.

    python -m retailpulse.medallion.gold
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from ..config import load_config
from ..session import cli, get_spark, save_all

log = logging.getLogger("gold")


def monthly_category_sales(enriched_sales: DataFrame) -> DataFrame:
    """B.14, C.1, C.2, C.6.

    Group by `category` and `order_month`; emit order_count, total_quantity,
    total_revenue plus avg/min/max order value, and a running revenue total
    per category ordered by month (sum over a rows-unbounded-preceding window).

    This is the table the Kafka events in Part E are built from, so it must
    carry: order_month, category, order_count, total_quantity, total_revenue.
    """
    raise NotImplementedError


def city_sales(enriched_sales: DataFrame) -> DataFrame:
    """B.14 -- revenue and order counts per city/state."""
    raise NotImplementedError


def customer_value(enriched_sales: DataFrame) -> DataFrame:
    """B.14, C.5, C.7.

    Lifetime revenue per customer, their latest order (row_number over
    order_timestamp desc), and a dense_rank of customers within each state.
    """
    raise NotImplementedError


def top_products_by_category(enriched_sales: DataFrame) -> DataFrame:
    """B.14, C.4 -- rank() products by revenue inside each category."""
    raise NotImplementedError


def main(mode: str | None = None) -> None:
    cfg = load_config(mode)
    spark = get_spark(cfg)
    enriched = spark.table(cfg.table("silver_enriched_sales"))

    save_all(
        cfg,
        {
            "gold_monthly_category_sales": monthly_category_sales(enriched),
            "gold_city_sales": city_sales(enriched),
            "gold_customer_value": customer_value(enriched),
            "gold_top_products_by_category": top_products_by_category(enriched),
        },
        log,
    )


if __name__ == "__main__":
    main(cli(__doc__))
