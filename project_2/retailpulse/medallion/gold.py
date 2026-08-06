"""Parts B.14 and C. Gold analytical tables and window functions.

    python -m retailpulse.medallion.gold
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window, WindowSpec
from pyspark.sql import functions as F

from ..config import load_config
from ..session import cli, get_spark, save_all

log = logging.getLogger("gold")


def monthly_category_sales(enriched_sales: DataFrame) -> DataFrame:
    """B.14, C.1, C.2, C.6.

    Part E builds its Kafka events from this table, so it has to keep
    order_month, category, order_count, total_quantity and total_revenue.
    """
    monthly_category_sales: DataFrame = enriched_sales.groupby(["category", "order_month"]).agg(
        F.count("*").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("net_sales").alias("total_revenue"),
        F.avg("net_sales").alias("avg_order_value"),
        F.min("net_sales").alias("min_order_value"),
        F.max("net_sales").alias("max_order_value"),
    )

    monthly_window: WindowSpec = (Window
                                  .partitionBy("category")
                                  .orderBy("order_month")
                                  .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    monthly_category_sales = (monthly_category_sales
                              .withColumn("running_total_revenue", F.sum("total_revenue").over(monthly_window)))


    return monthly_category_sales


def city_sales(enriched_sales: DataFrame) -> DataFrame:
    """B.14. Revenue and order counts per city/state."""

    city_sales: DataFrame = enriched_sales.groupby(["city", "state"]).agg(
        F.count("*").alias("order_count"),
        F.sum("net_sales").alias("total_revenue"),
    )

    return city_sales


def customer_value(enriched_sales: DataFrame) -> DataFrame:
    """B.14, C.5, C.7.

    Lifetime revenue per customer, their latest order, and a dense_rank of
    customers within each state.
    """

    customer_value: DataFrame = (enriched_sales
    .groupby(["customer_id", "state"])
    .agg(
        F.sum("net_sales").alias("lifetime_revenue"),
    ))

    # order_id/order_timestamp are gone after the aggregation above, so the latest
    # order has to come off the un-aggregated frame and be joined back on.
    order_window: WindowSpec = (Window.partitionBy("customer_id")
                                .orderBy(F.col("order_timestamp")
                                         .desc()))

    latest_order: DataFrame = (enriched_sales
                               .withColumn("_row_number", F.row_number().over(order_window))
                               .filter(F.col("_row_number") == 1)
                               .select("customer_id", F.col("order_id").alias("latest_order")))

    customer_rank_window: WindowSpec = (Window
                                        .partitionBy("state")
                                        .orderBy(F.col("lifetime_revenue")
                                                 .desc()))

    customer_value = (customer_value
                      .join(latest_order, on="customer_id", how="left")
                      .withColumn("state_rank", F.dense_rank().over(customer_rank_window)))

    return customer_value


def top_products_by_category(enriched_sales: DataFrame) -> DataFrame:
    """B.14, C.4. Rank products by revenue inside each category."""

    top_products_by_category: DataFrame = enriched_sales.groupby(
        ["category", "product_id", "product_name"]
    ).agg(
        F.count("*").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("net_sales").alias("total_revenue"),
    )

    product_rank_window: WindowSpec = (Window
                                       .partitionBy("category")
                                       .orderBy(F.col("total_revenue")
                                                .desc()))

    top_products_by_category = (top_products_by_category
                                .withColumn("revenue_rank", F.rank().over(product_rank_window)))

    return top_products_by_category


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
