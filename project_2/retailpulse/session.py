"""SparkSession construction, plus the two catalog calls shared by all layers.

This is the only module that branches on the run mode. Everything downstream
receives a plain SparkSession and cannot tell the difference.
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession

from .config import RunConfig


def get_spark(cfg: RunConfig) -> SparkSession:
    """Return a SparkSession appropriate for `cfg.mode`."""
    if cfg.is_local:
        return _local_session(cfg)
    return _databricks_session()


def _local_session(cfg: RunConfig) -> SparkSession:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")
    os.environ["PATH"] = (
        os.environ.get("PATH", "") + ";" + os.path.join(os.environ["HADOOP_HOME"], "bin")
    )
    cfg.local_warehouse.mkdir(parents=True, exist_ok=True)
    derby_home = cfg.local_warehouse.parent / "metastore_db"

    builder = (
        SparkSession.builder.appName("retailpulse-local")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", cfg.local_warehouse.as_posix())
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_home.as_posix()}")
        # 500-row inputs don't need 200 shuffle partitions.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _databricks_session() -> SparkSession:
    # Inside a Databricks notebook or job the runtime has already built the
    # session and databricks-connect is not installed, so reuse what's there.
    # Only a remote client has to open a Connect session, and it has no active
    # session on the first call. That makes this a better check than sniffing at
    # DATABRICKS_RUNTIME_VERSION, which the docs don't promise on serverless.
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    from databricks.connect import DatabricksSession

    builder = DatabricksSession.builder
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID", "").strip()
    if cluster_id:
        builder = builder.clusterId(cluster_id)
    else:
        # Free Edition is serverless-only.
        builder = builder.serverless(True)
    return builder.getOrCreate()


def ensure_namespace(spark: SparkSession, cfg: RunConfig) -> None:
    """Create the schema (and, on Databricks, the raw volume) if absent."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.namespace}")
    if not cfg.is_local:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.namespace}.{cfg.volume}")


def save(df: DataFrame, cfg: RunConfig, name: str) -> str:
    """Persist `df` as a managed Delta table and return its qualified name."""
    fqn = cfg.table(name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)
    return fqn


def save_all(cfg: RunConfig, tables: dict[str, DataFrame], logger: logging.Logger) -> None:
    """Write several tables and log the row count of each."""
    for name, df in tables.items():
        logger.info("wrote %s (%d rows)", save(df, cfg, name), df.count())


def cli(doc: str | None) -> str | None:
    """Shared `--mode` entrypoint plumbing. Pass the module's `__doc__`."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=(doc or "").strip().splitlines()[0])
    parser.add_argument("--mode", choices=["local", "databricks"], default=None)
    return parser.parse_args().mode
