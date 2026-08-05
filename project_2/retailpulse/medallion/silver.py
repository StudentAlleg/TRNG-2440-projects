"""Part B -- Silver layer: clean, cast, deduplicate, enrich.

Each function maps to a numbered requirement so the validation report can cite
row counts in and out.  Implement them one at a time; a test that builds a
5-row DataFrame and asserts on the result is the fastest way to prove each one.

    python -m retailpulse.medallion.silver
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, Window, WindowSpec
from pyspark.sql import functions as F

from ..config import load_config
from ..session import cli, get_spark, save_all

log = logging.getLogger("silver")

# Bronze stamps these onto every table (A.6), so all three frames carry them into
# the join.  Keep the order-level pair and drop the dimension copies.
LINEAGE_COLUMNS = ("source_file", "ingestion_timestamp")


def _normalize_strings(dataframe: DataFrame, *column_names: str) -> DataFrame:
    column_dict: dict[str, Column] = {name: F.initcap(F.trim(F.lower(F.col(name)))) for name in column_names}
    dataframe = dataframe.withColumns(column_dict)
    return dataframe


def clean_customers(bronze_customers: DataFrame) -> DataFrame:
    """B.2, B.3, B.4, B.5, B.6.

    Trim strings, fill missing `city` with 'Unknown', cast `signup_date` /
    `date_of_birth` to date and `updated_at` to timestamp, keep only the latest
    row per `customer_id` via row_number() over updated_at desc, then drop
    customers whose `is_active` is not 'Y'.
    """
    bronze_customers = bronze_customers.fillna({"city": "Unknown"})
    bronze_customers = _normalize_strings(bronze_customers, "customer_name",
                                          "city",
                                          "state",
                                          "region",
                                          "customer_segment",     
                                          )
    bronze_customers = bronze_customers.withColumns({
        #dates
        "signup_date": F.to_date(F.col("signup_date")),
        "date_of_birth": F.to_date(F.col("date_of_birth")),
        #timestamp
        "updated_at": F.to_timestamp(F.col("updated_at")),
        #int
        "loyalty_points": F.col("loyalty_points").cast("int")
    })
    window: WindowSpec = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

    bronze_customers = (bronze_customers.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number"))

    bronze_customers = bronze_customers.filter(F.col("is_active") == "Y")

    return bronze_customers


def clean_products(bronze_products: DataFrame) -> DataFrame:
    """B.2, B.4, B.7, B.8.

    Trim/standardise case, cast `unit_price` and `cost_price` to decimal(12,2)
    and `stock_quantity` to int, drop rows where either price is null or <= 0
    or cost > price, then drop rows whose `active_flag` is not 'Y'.
    """
    bronze_products = _normalize_strings(bronze_products,
                                         "product_name",
                                         "category",
                                         "subcategory",
                                         "supplier_name")
    bronze_products = bronze_products.withColumns({
        "unit_price": F.col("unit_price").cast("decimal(12,2)"),
        "cost_price": F.col("cost_price").cast("decimal(12,2)"),
        "stock_quantity": F.col("stock_quantity").cast("int")
    })

    bronze_products = bronze_products.dropna(subset=["unit_price", "cost_price"])
    bronze_products = bronze_products.filter((F.col("unit_price") > 0) & (F.col("cost_price") > 0) & (F.col("cost_price") <= F.col("unit_price")))
    bronze_products = bronze_products.filter(F.col("active_flag") == "Y")

    return bronze_products

def clean_orders(bronze_orders: DataFrame) -> DataFrame:
    """B.2, B.4, B.9, B.10.

    Cast `order_timestamp` to timestamp, delivery dates to date, `quantity` to
    int and `discount_pct` to decimal, drop quantity <= 0, then exclude
    PENDING and CANCELLED orders.
    """
    bronze_orders = bronze_orders.withColumns({
        "order_timestamp": F.to_timestamp(F.col("order_timestamp")),
        "promised_delivery_date": F.to_date(F.col("promised_delivery_date")),
        "actual_delivery_date": F.to_date(F.col("actual_delivery_date")),
        "quantity": F.col("quantity").cast("int"),
        "discount_pct": F.col("discount_pct").cast("decimal(5,2)")
    })

    bronze_orders = bronze_orders.filter((F.col("quantity") > 0) & (~F.col("order_status").isin(["PENDING", "CANCELLED"])))
    return bronze_orders


def enrich_sales(orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """B.11, B.12 -- join the three cleaned frames and add derived columns.

        gross_amount       quantity * unit_price
        discount_amount    gross_amount * discount_pct / 100
        net_amount         gross_amount - discount_amount
        net_sales          alias used by the Gold layer
        profit_per_unit    unit_price - cost_price
        delivery_days      datediff(actual_delivery_date, order_timestamp)
        late_delivery_flag actual_delivery_date > promised_delivery_date
        order_month        date_format(order_timestamp, 'yyyy-MM')
    """

    enriched_sales: DataFrame = (orders
                                 .join(products.drop(*LINEAGE_COLUMNS), on="product_id")
                                 .join(customers.drop(*LINEAGE_COLUMNS), on="customer_id"))
    enriched_sales = enriched_sales.withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
    enriched_sales = enriched_sales.withColumn("discount_amount", F.col("gross_amount") * F.col("discount_pct") / 100)
    enriched_sales = enriched_sales.withColumn("net_amount", F.col("gross_amount") - F.col("discount_amount"))
    enriched_sales = enriched_sales.withColumn("net_sales", F.col("net_amount"))
    enriched_sales = enriched_sales.withColumn("profit_per_unit", F.col("unit_price") - F.col("cost_price"))
    enriched_sales = enriched_sales.withColumn("delivery_days", F.datediff(F.col("actual_delivery_date"), F.col("order_timestamp")))
    enriched_sales = enriched_sales.withColumn("late_delivery_flag", F.col("actual_delivery_date") > F.col("promised_delivery_date"))
    enriched_sales = enriched_sales.withColumn("order_month", F.date_format(F.col("order_timestamp"), "yyyy-MM"))

    return enriched_sales


def main(mode: str | None = None) -> None:
    cfg = load_config(mode)
    spark = get_spark(cfg)

    customers = clean_customers(spark.table(cfg.table("bronze_customers")))
    products = clean_products(spark.table(cfg.table("bronze_products")))
    orders = clean_orders(spark.table(cfg.table("bronze_sales_orders")))

    save_all(
        cfg,
        {
            "silver_customers": customers,
            "silver_products": products,
            "silver_sales_orders": orders,
            "silver_enriched_sales": enrich_sales(orders, customers, products),
        },
        log,
    )


if __name__ == "__main__":
    main(cli(__doc__))
