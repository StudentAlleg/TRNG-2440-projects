"""Part A -- Bronze layer: raw CSV -> Delta, every column left as a string.

    python -m retailpulse.medallion.bronze
    python -m retailpulse.medallion.bronze --mode databricks
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession

from ..config import RAW_FILES, RunConfig, load_config
from ..session import cli, ensure_namespace, get_spark, save

log = logging.getLogger("bronze")


def read_raw(spark: SparkSession, cfg: RunConfig, dataset: str) -> DataFrame:
    """Read one raw CSV with no schema inference (the Bronze contract)."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(cfg.raw_path(dataset))
    )


def to_bronze(df: DataFrame, source_file: str) -> DataFrame:
    """Add `source_file` and `ingestion_timestamp` (requirement A.6)."""
    return df.withColumn("source_file", F.lit(source_file)).withColumn(
        "ingestion_timestamp", F.current_timestamp()
    )


def main(mode: Optional[str] = None) -> None:
    cfg: RunConfig = load_config(mode)
    spark: SparkSession = get_spark(cfg)
    ensure_namespace(spark, cfg)

    for dataset, filename in RAW_FILES.items():
        bronze: DataFrame = to_bronze(read_raw(spark, cfg, dataset), filename)
        log.info("wrote %s (%d rows)", save(bronze, cfg, f"bronze_{dataset}"), bronze.count())
        bronze.printSchema()
        bronze.show(5, truncate=False)


if __name__ == "__main__":
    main(cli(__doc__))
