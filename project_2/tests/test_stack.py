"""Smoke test: local Spark, Delta and the table read/write path.

If this passes, the stack is sound and every later failure is logic rather than
environment.
"""

from retailpulse.config import RAW_FILES
from retailpulse.medallion.bronze import read_raw, to_bronze
from retailpulse.session import ensure_namespace, save


def test_local_spark_delta_roundtrip(spark, cfg):
    ensure_namespace(spark, cfg)

    bronze = to_bronze(read_raw(spark, cfg, "customers"), RAW_FILES["customers"])
    save(bronze, cfg, "bronze_customers")

    loaded = spark.table(cfg.table("bronze_customers"))
    assert loaded.count() == 500
    assert {"source_file", "ingestion_timestamp"} <= set(loaded.columns)
